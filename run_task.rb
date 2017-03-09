class RunTask
  ALLOWED_STEP_NAMES = [:archive, :duplicity, :filter_data, :demultiplex].freeze

  def initialize(run_dir)
    @run_dir = run_dir
    @run_name = File.basename(@run_dir)
    @lock_file_name = "#{@run_dir}.lock"
    @log_file_name = File.join(BASECALL_DIR, ".log", "#{@run_name}.log")
    @sequencer = Sequencer.new(sn: `ls -ld #{@run_dir}`.split("\s")[2].sub('seq_', ''))

    # Error lock file, needs to be deleted by hand for the workflow to resume
    @error_file_name = "#{@run_dir}.err"
    # Skip lock file, makes the workflow skip one turn to check for Illumina's weird
    # way of notifying the user about (some) errors.
    @skip_file_name = "#{@run_dir}.skip"
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
      "%s, [%s #%d] (%s) %5s -- %s: %s\n" % [severity[0..0], datetime, $$, HOSTNAME, severity, progname, msg]
    end

    if DEBUG
      @logger.level = Logger::DEBUG
    else
      @logger.level = Logger::INFO
    end
    @logger
  end

end
