class NextSeq < Sequencer
  ALTERNATIVE_END_FILE = 'RunCompletionStatus.xml'

  def seq_complete?
    File.exists?(File.join(run_dir, 'RTAComplete.txt')) || File.exists?(File.join(run_dir, ALTERNATIVE_END_FILE))
  end

  def archive!
    unless lock_file_present?(lock_file_name)
      begin
        FileUtils.touch lock_file_name

        if seq_complete? && dir_forbidden?
          sync!

          year = "20#{run_name[0..1]}"
          local_archive_dir = File.join(Conf.global_conf[:basecall_dir], year)

          # final rsync done
          FileUtils.mkdir local_archive_dir unless File.directory?(local_archive_dir)

          new_run_dir = File.join(local_archive_dir, run_name) 

          # unless a directory with the same path exists:
          if !File.directory?(new_run_dir)

            # Move dir to final location and link back to /data/basecalls
            FileUtils.mv run_dir, new_run_dir
            logger.info "#{run_dir} moved to #{new_run_dir}"

            File.chmod 0755, new_run_dir
            logger.info "Changed permissions for #{new_run_dir} to 0755"

            FileUtils.ln_s new_run_dir, Conf.global_conf[:basecall_dir]
            logger.info "Aliased #{new_run_dir} to #{Conf.global_conf[:basecall_dir]}"

            ## restore users' access to sequencing files
            Find.find(new_run_dir) do |path|
              if File.directory?(path)
                File.chmod 0755, path
              else
                File.chmod 0744, path
              end
            end
            FileUtils.chown 'CF_Seq', 'deep_seq', File.join(Conf.global_conf[:basecall_dir], run_name)
            FileUtils.chown_R 'CF_Seq', 'deep_seq', new_run_dir

            Mailer.notify_run_finished(self)

            # guess what
            @run_dir = new_run_dir
            duplicity!

          # if it does but the run has already finished and this is just an
          # artifacts from the machine (thanks Illumina):
          elsif File.exists?(File.join(run_dir, ALTERNATIVE_END_FILE))
            # sync dup and delete
            logger.warn("Syncing sequencer artifact rundir, run duplicity again and delete the duplicates")

            source_dir = Shellwords.escape("#{run_dir}/.")
            FileUtils.cp_r(source_dir, new_run_dir)
            
            # reset users' permissions
            Find.find(new_run_dir) do |path|
              if File.directory?(path)
                File.chmod 0755, path
              else
                File.chmod 0744, path
              end
            end
            FileUtils.chown 'CF_Seq', 'deep_seq', File.join(Conf.global_conf[:basecall_dir], run_name)
            FileUtils.chown_R 'CF_Seq', 'deep_seq', new_run_dir

	    # needed for the duplicity call
	    old_run_dir = @run_dir
            @run_dir = new_run_dir
            duplicity!({single_step: true})

            FileUtils.remove_dir(old_run_dir)
            FileUtils.remove_dir(File.join(Conf.global_conf[:safe_location_dir], run_name), true)
          else
            raise Errors::DuplicateRunError.new("Duplicate run name detected (#{run_name})")
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
    else
      logger.warn "Lock file \"#{lock_file_name}\" still there, skipping."
    end
  end

end

