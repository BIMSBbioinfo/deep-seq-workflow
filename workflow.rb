require 'conf'
require 'mailer'
require 'run_task'

class Workflow
  include Conf

  def self.start(step)
    Dir.glob(File.join(global_conf[:basecall_dir], global_conf[:seq_dir_regexp], '*'), File::FNM_PATHNAME).select {|d| File.directory?(d) }.each do |run_dir|
      task = RunTask.new(run_dir)

      if step == :forbid
        # different, faster cronjob, this way we get less log noise
        task.forbid!
      else
        task.run_from(step)
      end

    end
  end

end
