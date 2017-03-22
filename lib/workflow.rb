require 'rsync'
require 'childprocess'
require 'fileutils'
require 'find'
require 'logger'
require 'net/smtp'
require 'yaml'
require 'shellwords'

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
      manager = Sequencer.new(run_dir)

      if step == :forbid
        # different, faster cronjob, this way we get less log noise
        manager.forbid!
      else
        manager.run_from(step)
      end

    end
  end

end
