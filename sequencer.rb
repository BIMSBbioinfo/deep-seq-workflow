class Sequencer
    
  attr_reader :task

  def initialize(task)
    @task = task
  end

  def self.select(options)
    case options[:sb][0]
    when 'M' then MiniSeq.new
    when 'N' then NextSeq.new
    when 'S' then HiSeq.new
    when 'K' then HiSeq.new
    else
      raise DSWErrors::UnknowMachineTypeError.new(options[:sb])
    end
  end

  def error?
    File.exists?(task.error_file_name)
  end

  def skip?
    File.exists?(task.skip_file_name)
  end

  # Did we already forbid access to the sequencing data?
  def dir_forbidden?
    if DEBUG
      File.stat(task.run_dir).mode.to_s(8) == "40755"
    else
      File.stat(task.run_dir).mode.to_s(8) == "100000" || File.stat(task.run_dir).mode.to_s(8) == "40000"
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
      notify_admins('lock_file_check', e)
    end
  end

  # Forbid access to the sequencing data once the sequencing is done.
  # Directory permissions are set to 0
  def forbid!
    lock_file_name = "#{task.run_dir}.forbid.lock"

    begin
      unless lock_file_present?(lock_file_name)
        if seq_complete?
          unless dir_forbidden?

            FileUtils.touch lock_file_name
            sleep 10
            File.chmod 0000, task.run_dir
            logger.info "Changed permissions for #{task.run_dir} to 0000"
          end
        end
      end
    rescue StandardError => e
      logger.error "while forbidding access to deep seq data:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      notify_admins('forbid_dir', e)
    ensure
      FileUtils.rm lock_file_name if lock_file_present?(lock_file_name)
    end
  end

  #
  # This function is a wrapper for the duplicity command to execute to create
  # a full remote backup.
  # The presence of the main lock is checked in order to avoid race conditions.
  # Moreover this function employs another lock file in order to track the
  # actual duplicity process execution.
  #
  def duplicity!(options ={})
    # this is just some idiom to declare some set of default options for the method
    # and adding whaterver overridden value comes down from the user.
    default_options = { single_step: false }
    default_options.merge!(options)

    duplicity_lock = "#{task.run_dir}.duplicity.lock"

    unless lock_file_present?(task.lock_file_name)
      begin
        unless lock_file_present?(duplicity_lock)
          logger.info "#{duplicity_lock} not found, setting up a new duplicity remote backup."
          FileUtils.touch task.lock_file_name

          # Duplicity-specific log file
          log_file_name = File.join(Conf.global_conf[:basecall_dir], ".log", "#{task.run_name}.duplicity.log")

          # Remote backup location access data
          archive_user = "bzpkuntz"
          archive_host = "mdcbio.zib.de"
          archive_dir  = "/mdcbiosam/archiv/solexa"
          local_duplicity_cache = File.join(global_conf[:basecall_dir], ".archive")

          # Default set of flag/value pairs
          # the final line joins key-value pairs with a '=' char 
          # i.e. '--ssh-backend=pexpect' and returns a list of such strings.
          duplicity_flags = {
            '--asynchronous-upload': nil,
            '--volsize': 1024,
            '--archive-dir': local_duplicity_cache,
            '--name': task.run_name,
            '--no-encryption': nil,
            '--tempdir': '/tmp',
            '--exclude': File.join(task.run_dir, 'demultiplexed_data'),
            '--verbosity': 8
          }.collect{ |kv| kv.compact.join('=') }

          # The actual command line string being built
          cmd_line = ['duplicity', 'full']
          cmd_line += duplicity_flags
          cmd_line += [task.run_dir,
                       "pexpect+sftp://#{archive_user}task.#{archive_host}/#{archive_dir}/#{task.run_name}"]

          log_file = File.open(log_file_name, 'a')

          logger.info "Duplicity command line:"
          logger.info cmd_line.join(' ')

          # Sub-process creation (see https://github.com/jarib/childprocess)
          duplicity_proc = ChildProcess.build(*cmd_line)

          # Detach it from the parent
          duplicity_proc.leader = true

          # XXX probably messes up the wait call later on.
          # duplicity_proc.detach = true

          # Assign output streams to the log file
          duplicity_proc.io.stdout = duplicity_proc.io.stderr = log_file

          # Start the data demultiplexing if all the conditions are met
          unless default_options[:single_step]
            if File.exists?(File.join(global_conf[:sample_sheets_dir], "#{task.run_name}.csv"))
              fork { demultiplex! }
            end
          end

          FileUtils.touch duplicity_lock

          # Start execution and wait for termination
          duplicity_proc.start
          logger.info "Started duplicity remote backup procedure. See '#{log_file_name}' for details."
          duplicity_proc.wait

          if duplicity_proc.exit_code == 0
            # Remove our duplicity-specific lock only on success
            FileUtils.rm duplicity_lock
            logger.info "Duplicity successfully completed a remote backup."

            log_file.close if log_file
            FileUtils.rm task.lock_file_name if lock_file_present?(task.lock_file_name)

            # Call next step if allowed
            # XXX check if some of the filtered data is needed by the demultiplexing step
            filter_data! unless default_options[:single_step]
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
        notify_admins("duplicity_function", e)
      ensure
        log_file.close if log_file
        FileUtils.rm task.lock_file_name if lock_file_present?(task.lock_file_name)
      end
    else
      logger.warn "Main lock file \"#{task.lock_file_name}\" still there, skipping."
    end
  end

  #
  # This function builds up a list of files to be deleted and deletes them.
  def filter_data!
    begin
      unless lock_file_present?(task.lock_file_name)
        FileUtils.touch task.lock_file_name

        # The find process command line
        find_dir = Shellwords.escape(task.run_dir)

        flist_cmd = %Q[find #{find_dir} -name '*' | \
egrep -i -e './Logs|./Images|RTALogs|reports|.cif|.cif.gz|.FWHMMap|_pos.txt|Converted-to-qseq']

        logger.info "Find command line:"
        logger.info flist_cmd

        # Runs the above command and saves the output in 'file_list';
        # reports eventual errors.
        file_list = %x[ #{flist_cmd} ]
        raise FindProcessError.new("'find' or 'grep' child processes exited with error status") if $?.exitstatus > 1

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
        Find.find(task.run_dir) do |path|
          if File.directory?(path)
            File.chmod 0755, path
          else
            File.chmod 0744, path
          end
        end
        FileUtils.chown 'CF_Seq', 'deep_seq', File.join(global_conf[:basecall_dir], task.run_name)
        FileUtils.chown_R 'CF_Seq', 'deep_seq', task.run_dir

        # Cleaning up the second copy of the data since the backup completed successfully
        logger.info "Removing backup from: #{File.join(global_conf[:safe_location_dir], task.run_name)}"
        FileUtils.remove_dir(File.join(global_conf[:safe_location_dir], task.run_name), true)

        notify_run_finished

      else
        logger.warn "Lock file \"#{task.lock_file_name}\" still there, skipping."
      end
    rescue StandardError => e
      logger.error "in filter data function:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      notify_admins("filter_data", e)
    ensure
      FileUtils.rm task.lock_file_name if File.exists?(task.lock_file_name)
    end

  end

end
