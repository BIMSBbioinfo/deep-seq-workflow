require 'rsync'
require 'childprocess'
require 'fileutils'
require 'find'
require 'logger'
require 'net/smtp'
require 'yaml'
require 'shellwords'
require 'csv'

ChildProcess.posix_spawn = true

require_relative 'dsw/conf'
require_relative 'dsw/mailer'
require_relative 'dsw/errors'
require_relative 'dsw/sequencer'
require_relative 'dsw/hi_seq'
require_relative 'dsw/next_seq'
require_relative 'dsw/mini_seq'

class Workflow
  include Errors

  def self.start(step)
    Dir.glob(File.join(Conf.global_conf[:basecall_dir], Conf.global_conf[:seq_dir_regexp], '*'), File::FNM_PATHNAME).select {|d| File.directory?(d) }.each do |run_dir|
      begin
        manager = Sequencer.new(run_dir)
      rescue Errors::EmptyRunDirPathError => erdpe
        logger.error("rundir path parameter is empty ('#{erdpe.message}'); Exiting with status 1.")
        Mailer.notify_admins(self, 'workflow_start', erdpe)
        exit(1)
      rescue Errors::UnknownMachineTypeError => umte
        logger.error("Unknown machine type extrapolated from directory name: '#{umte.message}'; Exiting with status 1.")
        Mailer.notify_admins(self, 'workflow_start', umte)
        exit(1)
      end

      if Dir["#{manager.run_dir}/*"].empty? && Dir.exist?(File.join(Conf.global_conf[:basecall_dir], manager.run_name))
        logger.warning("#{manager.run_dir} is empty and a rundir with the same name already exists in /data/basecalls: deleting #{manager.run_dir} (sequencer artifacts)")
        FileUtils.remove_dir(manager.run_dir, true)
      else
        if step == :forbid
          # different, faster cronjob, this way we get less log noise
          manager.forbid!
        else
          manager.run_from(step)
        end
      end

    end
  end

end
