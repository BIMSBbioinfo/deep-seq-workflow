require 'rsync'
require 'childprocess'

require 'fileutils'
require 'find'
require 'logger'
require 'net/smtp'
require 'yaml'
#require 'shellwords'

ChildProcess.posix_spawn = true

module DeepSeqWorkflow
  HOSTNAME = `hostname`.chomp
  # PREFIX = FileUtils.pwd
  PREFIX = '/'
  BASECALL_DIR = File.join(PREFIX, 'data', 'basecalls')
  SAFE_LOCATION_DIR = File.join(PREFIX, 'data', 'bc_copy')
  SAMPLE_SHEETS_DIR = File.join(PREFIX, 'data', 'basecalls', 'sample_sheets')
  DEBUG = false

  def self.start(step)
    Dir.glob(File.join(BASECALL_DIR, '.seq_*', '*'), File::FNM_PATHNAME).select {|d| File.directory?(d) }.each do |run_dir|
      task = DirTask.new(run_dir)

      if step == :forbid
        # different, faster cronjob, this way we get less log noise
        task.forbid!
      else
        task.run_from(step)
      end

    end
  end

  class DirTask
    ALLOWED_STEP_NAMES = [:archive, :duplicity, :filter_data, :demultiplex].freeze

    def initialize(run_dir)
      @run_dir = run_dir
      @run_name = File.basename(@run_dir)
      @lock_file_name = "#{@run_dir}.lock"
      @log_file_name = File.join(BASECALL_DIR, ".log", "#{@run_name}.log")
    end
    
    def to_yaml_properties
      instance_variables - [:@logger]
    end

    # Defines the logger instance for this process and a custom format for the loglines
    # so to include the hostname of the machine where this particualar instance is run.
    def logger
      @logger ||= Logger.new(@log_file_name)

      @logger.formatter = proc do |severity, datetime, progname, msg|
        "%s, [%s #%d] (%s) %5s -- %s: %s\n" % [severity[0..0], datetime, $$, HOSTNAME, severity, progname, msg]
      end

      if DEBUG
        @logger.level = Logger::DEBUG
      else
        @logger.level = Logger::INFO
      end
      @logger
    end

    # Has the sequencer finished its task?
    def seq_complete?
      File.exists?(File.join(@run_dir, 'RTAComplete.txt'))
    end

    # Did we already forbid access to the sequencing data?
    def dir_forbidden?
      if DEBUG
        File.stat(@run_dir).mode.to_s(8) == "40755"
      else
        File.stat(@run_dir).mode.to_s(8) == "100000" || File.stat(@run_dir).mode.to_s(8) == "40000"
      end
    end

    # Checks if a given lock file is present in the specified location.
    # On the side this function checks the status of the NFS shares because
    # if one of them is offline this function will throw an exception halting
    # the workflow.
    def lock_file_present?(lock_file_name)
      begin
        # TODO check lock file age and send a warning if it is older than a
        # given threshold.
        File.exists?(lock_file_name)
      rescue StandardError => e
        logger.error "checking lock file '#{lock_file_name}' presence:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins('lock_file_check', e)
        exit(1)
      end
    end

    # This is the function that kicks the workflow going.
    # Takes a step name from a list of allowed names and start the workflow
    # from there.
    def run_from(step)
      if ALLOWED_STEP_NAMES.include?(step)
        logger.info "[workflow_start] Starting deep seq data workflow from step: '#{step}'"
        logger.info self.to_yaml
        send("#{step}!")
        logger.info "[workflow_end] End of deep seq data workflow."
      else
        # illegal step parameter specified: notify and exit
        logger.error "Illegal step parameter specified: #{step}"
        logger.error "exiting with status 1"
        notify_admins('illegal_step')
        exit(1)
      end
    end

    # Forbid access to the sequencing data once the sequencing is done.
    # Directory permissions are set to 0
    def forbid!
      lock_file_name = "#{@run_dir}.forbid.lock"

      begin
        unless lock_file_present?(lock_file_name)
          if seq_complete?
            unless dir_forbidden?

              FileUtils.touch lock_file_name
              sleep 10
              File.chmod 0000, @run_dir
              logger.info "Changed permissions for #{@run_dir} to 0000"
            end
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
        FileUtils.rm lock_file_name if lock_file_present?(lock_file_name)
      end
    end

    private

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
        FileUtils.touch @lock_file_name

        rsync_type = seq_complete? ? 'final' : 'partial'
        logger.info "Starting #{rsync_type} rsync..."

        Rsync.run("#{@run_dir}/", File.join(SAFE_LOCATION_DIR, @run_name), '-raP') do |result|

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
        logger.error "exiting with status 1"
        notify_admins('sync', e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name if File.exists?(@lock_file_name)
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
      unless lock_file_present?(@lock_file_name)
        begin
          FileUtils.touch @lock_file_name

          if seq_complete? && dir_forbidden?
            sync!

            year = "20#{@run_name[0..1]}"
            local_archive_dir = File.join(BASECALL_DIR, year)

            # final rsync done
            FileUtils.mkdir local_archive_dir unless File.directory?(local_archive_dir)

            @new_run_dir = File.join(local_archive_dir, @run_name) 

            unless File.directory?(@new_run_dir)

              # Move dir to final location and link back to /data/basecalls
              FileUtils.mv @run_dir, @new_run_dir
              logger.info "#{@run_dir} moved to #{@new_run_dir}"
             
              File.chmod 0755, @new_run_dir
              logger.info "Changed permissions for #{@new_run_dir} to 0755"

              FileUtils.ln_s @new_run_dir, BASECALL_DIR
              logger.info "Aliased #{@new_run_dir} to #{BASECALL_DIR}"

            else
              raise DuplicateRunError("Duplicate run name detected (#{@run_name})")
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
          FileUtils.rm @lock_file_name if lock_file_present?(@lock_file_name)
          exit(1)
        end
      else
        logger.warn "Lock file \"#{@lock_file_name}\" still there, skipping."
        exit(0)
      end
    end

    #
    # This function is a wrapper for the duplicity command to execute to create
    # a full remote backup.
    # The presence of the main lock is checked in order to avoid race conditions.
    # Moreover this function employs another lock file in order to track the
    # actual duplicity process execution.
    #
    def duplicity!
      @new_run_dir ||= @run_dir

      duplicity_lock = "#{@new_run_dir}.duplicity.lock"

      unless lock_file_present?(@lock_file_name)
        begin
          unless lock_file_present?(duplicity_lock)
            logger.info "#{duplicity_lock} not found, setting up a new duplicity remote backup."
            FileUtils.touch @lock_file_name

            # Duplicity-specific log file
            log_file_name = File.join(BASECALL_DIR, ".log", "#{@run_name}.duplicity.log")

            # Remote backup location access data
            archive_user = "bzpkuntz"
            archive_host = "mdcbio.zib.de"
            archive_dir  = "/mdcbiosam/archiv/solexa"
            local_duplicity_cache = File.join(BASECALL_DIR, ".archive")

            # Default set of flag/value pairs
            duplicity_flags = {
              '--ssh-backend': 'pexpect',
              '--asynchronous-upload': nil,
              '--volsize': 1024,
              '--archive-dir': local_duplicity_cache,
              '--name': @run_name,
              '--no-encryption': nil,
              '--tempdir': '/tmp',
              '--verbosity': 8
            }.collect{ |kv| kv.compact.join('=') }
          
            # The actual command line string being built
            cmd_line = ['duplicity', 'full']
            cmd_line += duplicity_flags
            cmd_line += [@new_run_dir,
              "sftp://#{archive_user}@#{archive_host}/#{archive_dir}/#{@run_name}"]

            log_file = File.open(log_file_name, 'a')

            logger.info "Duplicity command line:"
            logger.info cmd_line.join(' ')

            # Sub-process creation (see https://github.com/jarib/childprocess)
            duplicity_proc = ChildProcess.build(*cmd_line)
            # Detach it from the parent
            duplicity_proc.leader = true
            duplicity_proc.detach = true
            # Assign output streams to the log file
            duplicity_proc.io.stdout = duplicity_proc.io.stderr = log_file

            FileUtils.touch duplicity_lock

            # Start execution and wait for termination
            duplicity_proc.start
            logger.info "Started duplicity remote backup procedure. See '#{log_file_name}' for details."
            duplicity_proc.wait

            if duplicity_proc.exit_code == 0
              # Remove duplicity-specific lock only on success
              FileUtils.rm duplicity_lock
              logger.info "Duplicity successfully completed a remote backup."

              log_file.close if log_file
              FileUtils.rm @lock_file_name if lock_file_present?(@lock_file_name)
              
              # Call next step
              filter_data!
            else
              raise DuplicityProcessError.new("'duplicity' exited with nonzero status\ncheck '#{log_file_name}' for details.")
            end

          else
            raise DuplicityLockError.new("Duplicity lock file is still present! Aborting workflow.")
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
          FileUtils.rm @lock_file_name if lock_file_present?(@lock_file_name)
        end
      else
        logger.warn "Main lock file \"#{@lock_file_name}\" still there, skipping."
        exit(0)
      end
    end

    #
    # This function builds up a list of files to be deleted and deletes them.
    def filter_data!
      @new_run_dir ||= @run_dir

      begin
        unless lock_file_present?(@lock_file_name)
          FileUtils.touch @lock_file_name

          # The find process command line
          flist_cmd = %Q[ find #{@new_run_dir} -name '*' | \
            egrep -i -e './Logs|./Images|RTALogs|reports|.cif|.cif.gz|.FWHMMap|_pos.txt|Converted-to-qseq']

          # Runs the above command and saves the output in 'file_list';
          # reports eventual errors.
          file_list = %x[ #{flist_cmd} ]
          raise FindProcessError.new("'find' child process exited with nonzero status") if $?.exitstatus != 0

          file_list = file_list.split("\n")

          logger.info "Removing the following files:"
          logger.info file_list

          # Removes the selected files, 100 at a time
          file_list.each_slice(100) do |slice|
            regulars = slice.reject {|path| File.directory?(path)}
            dirs = slice.select {|path| File.directory?(path)}
            FileUtils.rm(regulars)
            FileUtils.rmdir(dirs)
          end

          ## restore users' access to sequencing files
          Find.find(@new_run_dir) do |path|
            if File.directory?(path)
              File.chmod 0744, path
            else
              File.chmod 0755, path
            end
          end

          # Cleaning up the second copy of the data since the backup completed successfully
          logger.info "Removing backup from: #{File.join(SAFE_LOCATION_DIR, @run_name)}"
          FileUtils.remove_dir(File.join(SAFE_LOCATION_DIR, @run_name), true)

          notify_run_finished

        else
          logger.warn "Lock file \"#{@lock_file_name}\" still there, skipping."
          exit(0)
        end
      rescue StandardError => e
        logger.error "in filter data function:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins("filter_data", e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name if File.exists?(@lock_file_name)
      end

    end

    def demultiplex!
      @new_run_dir ||= @run_dir

      begin
        unless lock_file_present?(@lock_file_name)
          FileUtils.touch @lock_file_name
        else
          logger.warn "Lock file \"#{@lock_file_name}\" still there, skipping."
          exit(0)
        end
      rescue StandardError => e
        logger.error "in demultiplexing function:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        logger.error "exiting with status 1"
        notify_admins("demultiplex", e)
        exit(1)
      ensure
        FileUtils.rm @lock_file_name if File.exists?(@lock_file_name)
      end

    end

    def notify_admins(op, error =nil)
      unless DEBUG
        admins = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de"]
        admins.each do |adm|
          msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{adm}
Subject: [Deep Seq workflow] Error: #{op}
Date: #{Time.now}
          
Error code: #{op}
Run dir: #{@new_run_dir.nil? ? @run_dir : @new_run_dir}
Host: #{HOSTNAME}

See #{@log_file_name} for details.\n|

          unless error.nil?
            msg << "\n"
            msg << error.message
            msg << "\n"
            msg << error.backtrace.join("\n")
            msg << "\n"
          end

          msg << "\n---\ndsw"

          Net::SMTP.start('localhost', 25) do |smtp|
            smtp.send_message msg, 'dsw@mdc-berlin.net', adm
          end
        end
      else
        logger.debug "notify_admins(#{op})"
        logger.debug error.class
      end
    end

    def notify_run_finished
      unless DEBUG
        users = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de", "quedenau@mdc-berlin.de", "madlen.sohn@mdc-berlin.de"]
        users.each do |user|
          msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{user}
Subject: [Deep Seq workflow] Processing of run #{@run_name} finished
Date: #{Time.now}
          
Run dir: #{@new_run_dir.nil? ? @run_dir : @new_run_dir}

---\ndsw\n|

          Net::SMTP.start('localhost', 25) do |smtp|
            smtp.send_message msg, 'dsw@mdc-berlin.net', adm
          end
        end
      else
        logger.debug "Processing of run #{@run_name} finished"
      end

    end
  end

  class FindProcessError < StandardError; end
  class DuplicityProcessError < StandardError; end
  class DuplicityLockError < StandardError; end
  class RsyncProcessError < StandardError; end
  class DuplicateRunError < StandardError; end
end
