class HiSeq < Sequencer

    #
    # This function wraps the rsync phase be it either the partial or final one.
    # It first checks if there's already another workflow running for the current
    # run directory, in which case it just logs it and exit.
    # Otherwise:
    # - it creates a lock file
    # - calls rsync and waits for it to end
    # - logs and either returns or exit on errors (leaving the lock in place).
    #
    def sync!
      begin 
        FileUtils.touch task.lock_file_name

        rsync_type = seq_complete? ? 'final' : 'partial'
        logger.info "Starting #{rsync_type} rsync..."

        source_dir = Shellwords.escape("#{task.run_dir}/")
        dest_dir = Shellwords.escape(File.join(SAFE_LOCATION_DIR, task.run_name))

        Rsync.run(source_dir, dest_dir, '-raP') do |result|

          if result.success?
            result.changes.each do |change|
              logger.info "#{change.filename} (#{change.summary})"
            end
            logger.info "#{result.changes.count} change(s) sync'd."
            logger.info "End of #{rsync_type} rsync."
          else
            raise RsyncProcessError.new(
              "'rsync' exited with nonzero status (#{rsync_type} rsync), motivation: #{result.error}")
          end
        end

      rescue StandardError => e
        logger.error "#{e.class} encountered while performing the sync'ing step"
        logger.error e.message
        logger.error "trace:\n#{e.backtrace.join("\n")}"
        notify_admins('sync', e)
      ensure
        FileUtils.rm task.lock_file_name if File.exists?(task.lock_file_name)
      end
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
      unless lock_file_present?(task.lock_file_name)
        begin
          FileUtils.touch task.lock_file_name

          if seq_complete? && dir_forbidden?
            sync!

            year = "20#{task.run_name[0..1]}"
            local_archive_dir = File.join(BASECALL_DIR, year)

            # final rsync done
            FileUtils.mkdir local_archive_dir unless File.directory?(local_archive_dir)

            task.new_run_dir = File.join(local_archive_dir, task.run_name) 

            unless File.directory?(task.new_run_dir)

              # We need to skip at least one cron cycle (30') because the (new) Illumina sequencers
              # may write additional files after RTAComplete.txt if errors were
              # detected during the run.
              # The presence of task.skip_file_name will tell us that we need to skip the rundir
              # and the check for SequencingComplete will determine if we can further progress
              # or halt.

              # skip "lock" does not exist, create it and skip
              unless skip?
                FileUtils.touch task.skip_file_name
                raise SkipException
              else

                # If the skip "lock" exists check for SequencingComplete.txt:
                # - if it is there => errors occurred during the seq run => notify and abort
                # - if it is not there => delete lock and resume workflow
                # In case errors are detected, an error lock file is written to disk and the
                # rundir is exluded from the workflow until that file is deleted.

                if File.exists?(File.join(task.run_dir, "SequencingComplete.txt"))
                  FileUtils.touch task.error_file_name
                  raise SequencingError
                else
                  FileUtils.rm task.skip_file_name

                  # Move dir to final location and link back to /data/basecalls
                  FileUtils.mv task.run_dir, task.new_run_dir
                  logger.info "#{task.run_dir} moved to #{task.new_run_dir}"

                  File.chmod 0755, task.new_run_dir
                  logger.info "Changed permissions for #{task.new_run_dir} to 0755"

                  FileUtils.ln_s task.new_run_dir, BASECALL_DIR
                  logger.info "Aliased #{task.new_run_dir} to #{BASECALL_DIR}"

                  # guess what
                  duplicity!
                end

              end

            else
              raise DuplicateRunError("Duplicate run name detected (#{task.run_name})")
            end

          else
            logger.warn "Sequencing still running, just sync'ing."
            sync!
          end
        rescue SkipException => ske
          logger.info "Skipping the turn to check for run errors"
        rescue SequencingError => seqe
          logger.error "Errors were detected during the sequencing run; please refer to the log file(s) in: #{task.run_dir}/Logs"
          notify_run_errors(EVERYBODY)
        rescue StandardError => e
          logger.error "while performing the archiviation step:"
          logger.error e.message
          logger.error e.backtrace.join("\n")
          notify_admins('archive', e)
        ensure
          FileUtils.rm task.lock_file_name if lock_file_present?(task.lock_file_name)
        end
      else
        logger.warn "Lock file \"#{task.lock_file_name}\" still there, skipping."
      end
    end

end
