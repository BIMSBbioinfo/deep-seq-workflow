class HiSeq < Sequencer
  ALTERNATIVE_END_FILE = 'SequencingComplete.txt'

  def seq_complete?
    File.exists?(File.join(run_dir, 'RTAComplete.txt')) || File.exists?(File.join(run_dir, ALTERNATIVE_END_FILE))
  end

  #
  # This is the main function that gets the workflow going.
  # If the sequencing is done:
  # - calls the sync'ing function (final sync);
  # - moves the run directory to its final location and links it back to the
  #   basecalls directory while restoring access for the users;
  # - calls the duplicity archiving sub-routine.
  #
  # Otherwise it calls the sync'ing function and exits (partial sync)
  #
  def archive!
    if lock_file_present?(lock_file_name)
      logger.warn "Lock file \"#{lock_file_name}\" still there, skipping."
      return
    end

    begin
      FileUtils.touch lock_file_name

      if seq_complete? && dir_forbidden?
        sync!

        year = "20#{run_name[0..1]}"
        local_archive_dir = File.join(Conf.global_conf[:basecall_dir], year)

        # final rsync done
        FileUtils.mkdir local_archive_dir unless File.directory?(local_archive_dir)

        new_run_dir = File.join(local_archive_dir, run_name)

        if !File.directory?(new_run_dir)
          install_run(new_run_dir)
          @run_dir = new_run_dir
          Mailer.notify_run_finished(self)

          # guess what
          duplicity!

        # if it does but the run has already finished and this is just an
        # artifacts from the machine (thanks Illumina):
        elsif File.exists?(File.join(run_dir, ALTERNATIVE_END_FILE))
          # sync dup and delete
          logger.warn("Syncing sequencer artifact rundir, run duplicity again and delete the duplicates")

          source_dir = Shellwords.escape("#{run_dir}/.")
          FileUtils.cp_r(source_dir, new_run_dir)

          link = File.join(Conf.global_conf[:basecall_dir], run_name)
          fix_permissions(new_run_dir, link)

          # needed for the duplicity call
          old_run_dir = @run_dir
          @run_dir = new_run_dir
          duplicity!({single_step: true})

          FileUtils.remove_dir(old_run_dir)
          FileUtils.remove_dir(File.join(Conf.global_conf[:safe_location_dir], run_name), true)
        else
          raise Errors::DuplicateRunError("Duplicate run name detected (#{run_name})")
        end

      else
        logger.warn "Sequencing still running, just sync'ing."
        sync!
      end
    rescue => e
      logger.error "while performing the archiviation step:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      Mailer.notify_admins(self, 'archive', e)
    ensure
      FileUtils.rm lock_file_name if lock_file_present?(lock_file_name)
    end
  end

end

