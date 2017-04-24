class HiSeq < Sequencer

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

          unless File.directory?(new_run_dir)

            # We need to skip at least one cron cycle (30') because the (new) Illumina sequencers
            # may write additional files after RTAComplete.txt if errors were
            # detected during the run.
            # The presence of skip_file_name will tell us that we need to skip the rundir
            # and the check for SequencingComplete will determine if we can further progress
            # or halt.

            # skip "lock" does not exist, create it and skip
            unless skip?
              FileUtils.touch skip_file_name
              raise Errors::SkipException
            else

              # If the skip "lock" exists check for SequencingComplete.txt:
              # - if it is there => errors occurred during the seq run => notify and abort
              # - if it is not there => delete lock and resume workflow
              # In case errors are detected, an error lock file is written to disk and the
              # rundir is exluded from the workflow until that file is deleted.

              if File.exists?(File.join(run_dir, "SequencingComplete.txt"))
                FileUtils.touch error_file_name
                raise Errors::SequencingError
              else
                FileUtils.rm skip_file_name

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
                duplicity!
              end

            end

          else
            raise Errors::DuplicateRunError("Duplicate run name detected (#{run_name})")
          end

        else
          logger.warn "Sequencing still running, just sync'ing."
          sync!
        end
      rescue Errors::SkipException
        logger.info "Skipping the turn to check for run errors"
      rescue Errors::SequencingError
        logger.error "Errors were detected during the sequencing run; please refer to the log file(s) in: #{run_dir}/Logs"
        Mailer.notify_run_errors(self, Mailer::EVERYBODY)
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
