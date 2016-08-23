require 'rsync'
require 'childprocess'

require 'fileutils'
require 'logger'
require 'net/smtp'

ChildProcess.posix_spawn = true

module DeepSeqWorkflow
  @@hostname = `hostname`
  @@basecall_dir = File.join('/', 'data', 'basecalls')
  @@safe_location_dir = '/seq_data'

  def self.start
    Dir.glob(File.join(@@basecall_dir, '.seq_*', '*'), FNM_PATHNAME).collect(&:directory?).each do |run_dir|
      task = DirTask.new(run_dir)
      task.go!
    end
  end

  class DirTask
    def initialize(run_dir)
      @run_dir = run_dir
      @run_name = File.basename(@run_dir)
      @lock_file_name = "#{@run_dir}.lock"
      @log_file_name = File.join(@@basecall_dir, ".log", "#{@run_name}.log")
    end

    def logger
      @logger ||= Logger.new(@log_file_name)
      @logger.level = Logger::INFO
    end

    # Has the sequencer finished its task?
    def seq_complete?
      File.exists?(File.join(@run_dir, 'RTAComplete.txt'))
    end

    # Did we already forbid access to the sequencing data?
    def dir_forbidden?
      File.stat(@run_dir).mode.to_s(8) == "100000"
    end

    # This is the function that kicks the workflow going.
    def go!
      logger.info "[workflow_start] Starting deep seq data workflow..."
      logger.info self.to_yaml
      archive!
      logger.info "[workflow_end] End of deep seq data workflow."
    end

    def run_from_step(step)
      ALLOWED_STEP_NAMES = [:archive, :duplicity, :filter_data]
      if ALLOWED_STEP_NAMES.include?(step)
        send(step)
      else
        # illegal step parameter specified: notify and exit
        logger.error "Illegal step parameter specified: #{step}"
        logger.error "exiting with status 1"
        notify_admins('illegal_step')
        exit(1)
      end
    end

    # Forbid access to the sequencing data once the sequencing is done.
    def forbid!
      begin
        if seq_complete?
          unless dir_forbidden?

            FileUtils.touch @lock_file_name
            sleep 10
            File.chmod 0000, @run_dir
            logger.info "Changed permissions for #{@run_dir} to 0000"
          end
        end
      rescue StandardError => e
        logger.error "while forbidding access to deep seq data:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins('forbid_dir', e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name
      end
    end

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
      # check if the lock is already present in which case skip
      begin 
        unless File.exists?(@lock_file_name)
          FileUtils.touch @lock_file_name

          rsync_type = seq_complete? ? 'final' : 'partial'
          logger.info "Starting #{rsync_type} rsync..."

          Rsync.run(@run_dir, File.join(@@safe_location_dir, @run_name)) do |result|

            if result.success?
              result.changes.each do |change|
                logger.info "#{change.filename} (#{change.summary})"
              end
              logger.info "#{result.changes.count} change(s) sync'd."
              logger.info "End of #{rsync_type} rsync; Exiting Workflow..."
            else
              raise RsyncProcessError.new(
                "'rsync' exited with nonzero status (#{rsync_type} rsync)\n#{result.error}")
            end
          end

        else
          logger.warn "Lock file \"#{@lock_file_name}\" still there, skipping."
          exit(0)
        end

      rescue StandardError => e
        logger.error "while performing the sync'ing step:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins('sync', e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name
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
      begin
        if seq_complete? && dir_forbidden?
          # sync! takes care of checking the lock file presence and quits in
          # case it's there.
          sync!

          year = "20#{@run_name[0..1]}"
          local_archive_dir = File.join(@@basecall_dir, year)
          FileUtils.touch @lock_file_name

          # final rsync done
          FileUtils.mkdir local_archive_dir unless File.directory?(local_archive_dir)

          @new_run_dir = File.join(local_archive_dir, @run_name) 

          unless File.directory?(@new_run_dir)

            # Move dir to final location and link back to /data/basecalls
            FileUtils.mv @run_dir, @new_run_dir
            logger.info "#{@run_dir} moved to #{@new_run_dir}"
           
            File.chmod 0755, @new_run_dir
            logger.info "Changed permissions for #{@new_run_dir} to 0755"

            FileUtils.ln_s @new_run_dir, @@basecall_dir
            logger.info "Aliased #{@new_run_dir} to #{@@basecall_dir}"

          else
            raise DuplicityProcessError("Duplicate run name detected (#{@run_name})")
          end

          duplicity!

        else
          logger.warn "Sequencing still running, just sync'ing."
          sync!
          exit(0)
        end
      rescue StandardError => e
        logger.error "while performing the archiviation step:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins('archive', e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name
      end
    end

    # WIP doc
    def duplicity!
      duplicity_lock = "#{@run_dir}.duplicity.lock"

      begin
        unless File.exists?(@lock_file_name)
          unless File.exists?(duplicity_lock)
            FileUtils.touch @lock_file_name

            log_file_name = File.join(@@basecall_dir, ".log", "#{@run_name}.duplicity.log")

            archive_user = "bzpkuntz"
            archive_host = "mdcbio.zib.de"
            archive_dir  = "/mdcbiosam/archiv/solexa_gpg_test"
            local_duplicity_cache="/data/basecalls/.archive"

            duplicity_flags = {
              '--archive-dir': local_duplicity_cache,
              '--name': @run_name,
              '--no-encryption': nil,
              '--temp-dir': '/tmp',
              '--verbosity': "'1'"
            }.collect{ |kv| kv.join(' ') }
          
            cmd_line = ['duplicity', 'full']
            cmd_line += duplicity_flags
            cmd_line += [@new_run_dir,
              "sftp://#{archive_user}@#{archive_host}#{archive_dir}/#{@run_name}"]

            log_file = File.open(log_file_name, 'a')

            duplicity = ChildProcess.build(*cmd_line)
            duplicity.leader = true
            duplicity.detach = true
            duplicity.io.stdout = duplicity.io.stderr = log_file

            FileUtils.touch duplicity_lock
            duplicity.start
            duplicity.wait

            if duplicity.exit_code == 0
              logger.info "Duplicity successfully completed a remote backup"
              filter_data!
            else
              raise DuplicityProcessError.new("'duplicity' exited with nonzero status\ncheck '#{log_file_name}' for details.")
            end

          else
            raise DuplicityLockError.new("Duplicity lock file is still present! Aborting workflow.")
          end
        else
          logger.warn "Main lock file \"#{@lock_file_name}\" still there, skipping."
          exit(0)
        end
      rescue StandardError => e
        logger.error "in duplicity backup function:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins("duplicity_function", e)
        exit(1)
      ensure
        log_file.close if log_file
        FileUtils.rm @lock_file_name
      end
    end

    # WIP doc
    def filter_data!
      begin
        unless File.exists?(@lock_file_name)
          FileUtils.touch @lock_file_name

          flist_cmd = %Q[ find ./ -name '*' | \
            egrep -i -e './Logs|./Images|RTALogs|reports|.cif|.cif.gz|.FWHMMap|_pos.txt|Converted-to-qseq' | \
            sed 's/^..//' | \
            xargs ]

          file_list = %x[ #{flist_cmd} ]
          raise FindProcessError.new("'find' child process exited with nonzero status") if $?.exitstatus != 0

          file_list = file_list.split("\n")

          file_list.each_slice(100) do |slice|
            FileUtils.rm(slice.join(''))
          end

          # cleaning up the second copy of the data since the backup completed successfully
          FileUtils.rm(FileUtils.join(@@safe_location_dir, @run_name))

        else
          logger.warn "Lock file \"#{@lock_file_name}\" still there, skipping."
          exit(0)
        end
      rescue StandardError => e
        logger.error "in filer data function:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins("duplicity_function", e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name
      end

    end

    def notify_admins(op, error)
      admins = ["carlomaria.massimo@mdc-berlin.de" "dan.munteanu@mdc-berlin.de"]
      admins.each do |adm|
        msg = %Q|
        From: deep_seq_workflow <dsw@mdc-berlin.net>
        To: #{adm}
        Subject: [Deep Seq workflow] Error: #{op}
        Date: #{Time.now}
        
        Error code: #{op}
        Run dir: #{@new_run_dir.nil? ? @run_dir : @new_run_dir}
        Host: #{@@hostname}

        See #{@lock_file_name} for details.\n|

        unless error.nil?
          msg << "\n"
          msg << error.message
          msg << "\n"
          msg << error.backtrace.join("\n")
          msg << "\n"
        end

        msg << "\n---\ndsw"

        Net::SMTP.start('your.smtp.server', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', adm
        end
      end
    end
  end

  class FilterProcessError < StandardError; end
  class DuplicityProcessError < StandardError; end
  class DuplicityLockError < StandardError; end
  class RsyncProcessError < StandardError; end
end
