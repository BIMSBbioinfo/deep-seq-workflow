require "csv"

class Sequencer
    
  attr_reader :run_dir, :run_name, :lock_file_name, :logger, :log_file_name

  def initialize(rundir)
    if rundir.empty?
      raise Errors::EmptyRunDirPathError.new(rundir)
    else
      @run_dir = rundir
      @run_name = File.basename(@run_dir)
      @lock_file_name = "#{@run_dir}.lock"
      @log_file_name = File.join(Conf.global_conf[:log_dir], "#{@run_name}.log")
    end
  end

  def self.select(run_dir)
    serial_no = `ls -ld #{Shellwords.escape(run_dir)}`.split("\s")[2].sub('seq_', '')

    # rundir has already been moved to /data/basecalls
    if serial_no == 'CF_Seq'
      serial_no = File.basename(run_dir).split('_')[1]
    end

    case serial_no[0]
    when 'M' then MiniSeq.new(run_dir)
    when 'N' then NextSeq.new(run_dir)
    when 'S' then HiSeq.new(run_dir)
    when 'K' then HiSeq.new(run_dir)
    when 'P' then Pacbio.new(run_dir)
    else
      raise Errors::UnknownMachineTypeError.new(serial_no)
    end
  end

  ALLOWED_STEP_NAMES = [:archive, :duplicity, :filter_data, :demultiplex].freeze

  # This is the function that kicks the workflow going.
  # Takes a step name from a list of allowed names and start the workflow
  # from there.
  def run_from(step, options =nil)
    if ALLOWED_STEP_NAMES.include?(step)
      logger.info "[workflow_start] Starting deep seq data workflow on machine: #{self.class.name}, from step: '#{step}'"
      logger.info self.to_yaml
      if options.nil?
        send("#{step}!")
      else
        send("#{step}!", options)
      end
      logger.info "[workflow_end] End of deep seq data workflow."
    else
      # illegal step parameter specified: notify and exit
      logger.error "Illegal step parameter specified: #{step}"
      notify_admins('illegal_step')
    end
  end

  # Get rid of some log noise.
  def to_yaml_properties
    instance_variables - [:@logger]
  end

  # Defines the logger instance for this process and a custom format for the loglines
  # so to include the hostname of the machine where this particualar instance is run.
  def logger
    @logger ||= Logger.new(@log_file_name)

    @logger.formatter = proc do |severity, datetime, progname, msg|
      "%s, [%s #%d] (%s) %5s -- %s: %s\n" % [severity[0..0], datetime, $$, Conf.global_conf[:hostname], severity, progname, msg]
    end

    if Conf.global_conf[:debug]
      @logger.level = Logger::DEBUG
    else
      @logger.level = Logger::INFO
    end
    @logger
  end

  # XXX now it is the same as the mdc_archive_dir so it's fine but this code is
  # not correct.
  def archive_dir
    Conf.global_conf[:zib_archive_dir]
  end

  def seq_complete?
    File.exists?(File.join(run_dir, 'RTAComplete.txt'))
  end

  # Checks wether access to the sequencing data has already been forbidden.
  def dir_forbidden?
    File.stat(run_dir).mode.to_s(8) == "100000" || File.stat(run_dir).mode.to_s(8) == "40000"
  end

  # Checks if a given lock file is present in the specified location.
  # On the side this function checks the status of the NFS shares because
  # if one of them is offline this function will throw an exception halting
  # the workflow.
  def lock_file_present?(lfname)
    begin
      # TODO check lock file age and send a warning if it is older than a
      # given threshold.
      File.exists?(lfname)
    rescue => e
      logger.error "checking lock file '#{lfname}' presence:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      Mailer.notify_admins(self, 'lock_file_check', e)
    end
  end

  # Forbid access to the sequencing data once the sequencing is done.
  # Directory permissions are set to 0
  def forbid!
    forbid_lock_file_name = "#{run_dir}.forbid.lock"

    begin
      unless lock_file_present?(forbid_lock_file_name)
        if seq_complete?
          unless dir_forbidden?

            FileUtils.touch forbid_lock_file_name
            sleep 10
            File.chmod 0000, run_dir
            logger.info "Changed permissions for #{run_dir} to 0000"
          end
        end
      end
    rescue => e
      logger.error "while forbidding access to deep seq data:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      Mailer.notify_admins(self, 'forbid_dir', e)
    ensure
      FileUtils.rm forbid_lock_file_name if lock_file_present?(forbid_lock_file_name)
    end
  end

  #
  # This function wraps the rsync phase be it either the partial or final one.
  # It first checks if there's already another workflow running for the current
  # run directory, in which case it just logs it and exit.
  # Otherwise:
  # - it creates a lock file
  # - calls rsync and waits for it to end
  # - logs changes and errors
  #
  def sync!
    begin 
      FileUtils.touch lock_file_name

      rsync_type = seq_complete? ? 'final' : 'partial'
      logger.info "Starting #{rsync_type} rsync..."

      source_dir = Shellwords.escape("#{run_dir}/")
      dest_dir = Shellwords.escape(File.join(Conf.global_conf[:safe_location_dir], run_name))

      Rsync.run(source_dir, dest_dir, '-raP') do |result|

        if result.success?
          result.changes.each do |change|
            logger.info "#{change.filename} (#{change.summary})"
          end
          logger.info "#{result.changes.count} change(s) sync'd."
          logger.info "End of #{rsync_type} rsync."
        else
          raise Errors::RsyncProcessError.new(
            "'rsync' exited with nonzero status (#{rsync_type} rsync), motivation: #{result.error}")
        end
      end

    rescue => e
      logger.error "#{e.class} encountered while performing the sync'ing step"
      logger.error e.message
      logger.error "trace:\n#{e.backtrace.join("\n")}"
      Mailer.notify_admins(self, 'sync', e)
    ensure
      FileUtils.rm lock_file_name if File.exists?(lock_file_name)
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

        raise Errors::DuplicateRunError.new("Duplicate run name detected (#{run_name})") if File.directory?(new_run_dir)

        install_run(new_run_dir)
        @run_dir = new_run_dir
        Mailer.notify_run_finished(self)

        # guess what
        duplicity!

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

    duplicity_lock = "#{run_dir}.duplicity.lock"

    unless lock_file_present?(lock_file_name)
      begin
        unless lock_file_present?(duplicity_lock)
          logger.info "#{duplicity_lock} not found, setting up a new duplicity remote backup."
          FileUtils.touch lock_file_name

          # Duplicity-specific log file
          dup_log_file_name = File.join(Conf.global_conf[:log_dir], "#{run_name}.duplicity.log")

          # Local tape archive location access data
          archive_user = Conf.global_conf[:mdc_archive_user]
          archive_host = Conf.global_conf[:mdc_archive_host]
          local_duplicity_cache = Conf.global_conf[:local_dup_cache_dir1]

          # Default set of flag/value pairs
          # the final line joins key-value pairs with a '=' char 
          # i.e. '--ssh-backend=pexpect' and returns a list of such strings.
          duplicity_flags = {
            '--asynchronous-upload': nil,
            '--volsize': 1024,
            '--archive-dir': local_duplicity_cache,
            '--name': run_name,
            '--no-encryption': nil,
            '--tempdir': '/tmp',
            '--exclude': File.join(run_dir, 'demultiplexed_data'),
            '--verbosity': 8,
            '--allow-source-mismatch': nil
          }.collect{ |kv| kv.compact.join('=') }

          # The actual command line string being built
          cmd_line = ['duplicity']
          cmd_line += duplicity_flags
          cmd_line += [run_dir,
                       "pexpect+sftp://#{archive_user}@#{archive_host}/#{archive_dir}/#{run_name}"]

          log_file = File.open(dup_log_file_name, 'a')

          logger.info "Duplicity command line:"
          logger.info cmd_line.join(' ')

          # Sub-process creation (see https://github.com/jarib/childprocess)
          duplicity_proc = ChildProcess.build(*cmd_line)

          # By default, the child process does not create a new
          # process group. This means there's no guarantee that the
          # entire process tree will die when the child process is
          # killed. To solve this we set "leader".
          duplicity_proc.leader = true

          # Assign output streams to the log file
          duplicity_proc.io.stdout = duplicity_proc.io.stderr = log_file

          # Start the data demultiplexing if we are not single
          # stepping and the samplesheet for the run exists.
          unless default_options[:single_step]
            if File.exists?(File.join(Conf.global_conf[:sample_sheets_dir], "#{run_name}.csv"))
              fork { demultiplex! }
            end
          end

          FileUtils.touch duplicity_lock

          # Start execution and wait for termination
          duplicity_proc.start
          logger.info "Started duplicity remote backup procedure. See '#{dup_log_file_name}' for details."
          duplicity_proc.wait

          if duplicity_proc.exit_code == 0
            # Remove our duplicity-specific lock only on success
            FileUtils.rm duplicity_lock
            logger.info "Duplicity successfully completed a remote backup."

            log_file.close if log_file
            FileUtils.rm lock_file_name if lock_file_present?(lock_file_name)

            # Call next step if allowed
            filter_data! unless default_options[:single_step]
          else
            raise Errors::DuplicityProcessError.new("'duplicity' exited with nonzero status\ncheck '#{dup_log_file_name}' for details.")
          end

        else
          raise Errors::DuplicityLockError.new("Duplicity lock file is still present! Aborting workflow.")
        end
      rescue => e
        logger.error "in duplicity backup function:"
        logger.error e.message
        logger.error e.backtrace.join("\n")
        Mailer.notify_admins(self, "duplicity_function", e)
      ensure
        log_file.close if log_file
        FileUtils.rm lock_file_name if lock_file_present?(lock_file_name)
      end
    else
      logger.warn "Main lock file \"#{lock_file_name}\" still there, skipping."
    end
  end

  #
  # This function builds up a list of files to be deleted and deletes them.
  def filter_data!
    begin
      unless lock_file_present?(lock_file_name)
        FileUtils.touch lock_file_name

        # The find process command line
        find_dir = Shellwords.escape(run_dir)

        flist_cmd = %Q[find #{find_dir} -name '*' | \
egrep -i -e './Logs|./Images|RTALogs|reports|.cif|.cif.gz|.FWHMMap|_pos.txt|Converted-to-qseq']

        logger.info "Find command line:"
        logger.info flist_cmd

        # Runs the above command and saves the output in 'file_list';
        # reports eventual errors.
        file_list = %x[ #{flist_cmd} ]
        raise Errors::FindProcessError.new("'find' or 'grep' child processes exited with error status") if $?.exitstatus > 1

        file_list = file_list.split("\n")

        logger.info "Removing the following files:"
        logger.info file_list

        # Remove all regular files first.
        regulars = file_list.reject {|path| File.directory?(path)}
        regulars.each_slice(100) do |slice|
          FileUtils.rm(slice)
        end

        # ...and then remove all the empty directories, from the
        # inside out.
        dirs = file_list.select {|path| File.directory?(path)}.
                 sort_by {|dir| dir.count('/') }.reverse
        dirs.each_slice(100) do |slice|
          FileUtils.rmdir(slice)
        end

        # Cleaning up the second copy of the data since the backup completed successfully
        logger.info "Removing backup from: #{File.join(Conf.global_conf[:safe_location_dir], run_name)}"
        FileUtils.remove_dir(File.join(Conf.global_conf[:safe_location_dir], run_name), true)

      else
        logger.warn "Lock file \"#{lock_file_name}\" still there, skipping."
      end
    rescue => e
      logger.error "in filter data function:"
      logger.error e.message
      logger.error e.backtrace.join("\n")
      Mailer.notify_admins(self, "filter_data", e)
    ensure
      FileUtils.rm lock_file_name if File.exists?(lock_file_name)
    end

  end

  # manual restore function
  def self.restore!(options ={})
    default_options = {}
    default_options.merge!(options)

    # run_name is a field of the Sequencer class so this local var has been named:
    runname = map_long_name_to_short(options[:subdir], options[:run_name])
    # the above function call will search the map and return the short name
    # corresponding to the long name provided or return the input untouched
    # otherwise.

    begin
      # Duplicity-specific log file
      dup_log_file_name = File.join(Conf.global_conf[:log_dir], "#{runname}.restore.log")

      # Remote backup location access data
      archive_user = Conf.global_conf[:zib_archive_user]
      archive_host = Conf.global_conf[:zib_archive_host]
      local_duplicity_cache = Conf.global_conf[:local_dup_cache_dir]
      dest_dir = File.join(Conf.global_conf[:restore_dir], options[:run_name])

      # TODO: select an appropriate class dependent on the subdir
      # option, so that we can use manager.archive_dir instead
      manager = Sequencer.new(dest_dir)

      # TODO: use config value here instead of "/mdcbiosam/archiv"
      archive_dir = "/mdcbiosam/archiv/#{options[:subdir]}"
      duplicity_lock = "#{dest_dir}.duplicity.lock"

      # Default set of flag/value pairs
      # the final line joins key-value pairs with a '=' char 
      # i.e. '--tmpdir=/tmp' and returns a list of such strings.
      duplicity_flags = {
        '--archive-dir': local_duplicity_cache,
        '--name': runname,
        '--numeric-owner': nil,
        '--no-encryption': nil,
        '--tempdir': '/tmp',
        '--verbosity': 8,
        '--allow-source-mismatch': nil
      }.collect{ |kv| kv.compact.join('=') }

      # The actual command line string being built
      cmd_line = ['duplicity', 'restore']
      cmd_line += duplicity_flags
      cmd_line += ["pexpect+sftp://#{archive_user}@#{archive_host}/#{archive_dir}/#{runname}",
                  dest_dir]

      log_file = File.open(dup_log_file_name, 'a')

      manager.logger.info "Duplicity command line:"
      manager.logger.info cmd_line.join(' ')

      # Sub-process creation (see https://github.com/jarib/childprocess)
      duplicity_proc = ChildProcess.build(*cmd_line)

      # Detach it from the parent
      duplicity_proc.leader = true

      # Assign output streams to the log file
      duplicity_proc.io.stdout = duplicity_proc.io.stderr = log_file

      FileUtils.touch duplicity_lock

      # Start execution and wait for termination
      duplicity_proc.start
      manager.logger.info "Started duplicity remote restore procedure. See '#{dup_log_file_name}' for details."
      duplicity_proc.wait

      if duplicity_proc.exit_code == 0
        # Remove our duplicity-specific lock only on success
        FileUtils.rm duplicity_lock
        manager.logger.info "Duplicity successfully completed a restore operation."

        log_file.close if log_file

        # TODO notify somebody
        
        # Link the restored directory back to the basecall main dir.
        FileUtils.ln_s dest_dir, Conf.global_conf[:basecall_dir]

        link = File.join(Conf.global_conf[:basecall_dir], runname)
        fix_permissions(dest_dir, link)

      else
        raise Errors::DuplicityProcessError.new("'duplicity' exited with nonzero status\ncheck '#{dup_log_file_name}' for details.")
      end

    rescue => e
      manager.logger.error "in duplicity restore function:"
      manager.logger.error e.message
      manager.logger.error e.backtrace.join("\n")
      Mailer.notify_admins(manager, "restore_function", e)
    ensure
      log_file.close if log_file
    end
  end

  def self.map_long_name_to_short(subdir, rname)
    dir = File.expand_path File.dirname(__FILE__)
    map_file = "#{dir}/map-#{subdir}.csv"
    unless File.exists?(map_file)
      raise Errors::UnknownMapFile.new("There is no map file for #{subdir}.")
    end
    name_map = CSV.read(map_file)
    res = name_map.find {|key,value| key == rname }
    if res
      res[1]
    else
      rname
    end
  end

  # Move the current run_dir to TARGET, the absolute directory name
  # (including the year).  It also creates a convenient link to the
  # basecall directory.
  def install_run(target)
    # TODO: move atomically
    FileUtils.mv run_dir, target
    logger.info "#{run_dir} moved to #{target}"

    File.chmod 0755, target
    logger.info "Changed permissions for #{target} to 0755"

    FileUtils.ln_s target, Conf.global_conf[:basecall_dir]
    logger.info "Aliased #{target} to #{Conf.global_conf[:basecall_dir]}"

    link = File.join(Conf.global_conf[:basecall_dir], self.run_name)
    fix_permissions(target, link)
  end

  def fix_permissions(target, link)
    # Grant read/write access to the sequencing files to the "CF_Seq"
    # user and the "deep_seq" group.
    Find.find(target) do |path|
      if File.directory?(path)
        File.chmod 0755, path
      else
        File.chmod 0744, path
      end
    end
    FileUtils.chown 'CF_Seq', 'deep_seq', link
    FileUtils.chown_R 'CF_Seq', 'deep_seq', target
    return true
  end
end
