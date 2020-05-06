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

  def self.select_manager(run_dir)
    begin
      manager = Sequencer.select(run_dir)
    rescue Errors::EmptyRunDirPathError => erdpe
      puts("rundir path parameter is empty ('#{erdpe.message}'); Exiting with status 1.")
      Mailer.notify_admins(Sequencer.new(run_dir), 'workflow_start', erdpe)
      exit(1)
    rescue Errors::UnknownMachineTypeError => umte
      puts("Unknown machine type extrapolated from directory name: '#{umte.message}'; Exiting with status 1.")
      Mailer.notify_admins(Sequencer.new(run_dir), 'workflow_start', umte)
      exit(1)
    end

    return manager
  end

  # removes an empty rundir if it is just an artificial duplicate due to the sequencer software that keeps writing on the
  # remote share.
  def self.remove_dir_if_empty(manager)
    if Dir.exists?(manager.run_dir) &&
       Dir["#{manager.run_dir}/*"].empty? &&
       Dir.exist?(File.join(Conf.global_conf[:basecall_dir], manager.run_name))
      manager.logger.warn("#{manager.run_dir} is empty and a rundir with the same name already exists in /data/basecalls: deleting #{manager.run_dir} (sequencer artifacts)")
      FileUtils.remove_dir(manager.run_dir, true)
      return true
    else
      return false
    end
  end
 
  def self.start(step)
    Dir.glob(File.join(Conf.global_conf[:basecall_dir],
                       Conf.global_conf[:seq_dir_regexp],
                       '*'),
             File::FNM_PATHNAME).
      select {|d| File.directory?(d) }.
      each do |run_dir|

      # if the dir is an artifact, another process may have cleaned it up in the
      # meanwhile so we should skip it.
      if Dir.exists?(run_dir) && ! File.exists?("#{run_dir}.lock")
        manager = select_manager(run_dir)

        unless remove_dir_if_empty(manager)
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

end
