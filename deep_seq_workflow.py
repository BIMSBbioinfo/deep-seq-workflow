from subprocess import call, run, PIPE
#from configobj import ConfigObj
import logging
import glob
from os import path, stat
import sys, stat, time
from dsw_settings import load_config

# XXX run_dir needs to be escaped for white spaces
class DirTask:
    def __init__(self, run_dir):
        self.settings = load_config()
        self.run_dir = run_dir
        self.run_name = path.basename(self.run_dir)
        Self.lock_file_name = "%s.lock" % self.run_dir
        self.log_file_name = path.join(config('basecall_dir'), '.log', "%s.log" % self.run_name)

        Self.error_file_name = "%s.err" % self.run_dir
        Self.skip_file_name = "%s.skip" % self.run_dir

        if config('debug'):
            log_level = logging.DEBUG
        else:
            log_level = logging.INFO

        logging.basicConfig(
                filename=self.log_file_name,
                level=log_level,
                format="%(levelno)s [%(asctime)s #%(process)d] (%s) %(levelname)s -- %(processName)s: %(message)s" % config('hostname'))

    def __str__(self):
        return str(self.__dict__)

    def config(self, key):
        return self.settings[key]

    def has_errors(self):
        return path.exists(self.error_file_name)

    def to_skip(self):
        return path.exists(self.skip_file_name)

    def seq_complete(self):
        return path.exist(path.join(self.run_dir, 'RTAComplete.txt'))

    def dir_forbidden(self):
        mode = os.stat(self.run_dir).st_mode
        return not bool(mode & stat.S_IRWXU and mode & stat.S_IRWXG and mode & stat.S_IRWXO) 

    def lock_file_present(lock_file_name):
        try:
            # TODO check lock file age and send a warning if it is older than a
           # given threshold.
           path.exists(lock_file_name)
        except IOError:
            logging.exception("checking lock file '%s' presence:" % self.lock_file_name)
#           notify_admins('lock_file_check', e)
            sys.exit(1)

    def run_from(self, step, options =None):
        # Error lock file presence vincit omnia
        if not self.has_errors():
            if step in config('allowed_step_names'):
                logging.info("[workflow_start] Starting deep seq data workflow from step: '%s'" % step)
                logging.info(self)
                if options is None:
                    send("#{step}!")
                else:
                    send("#{step}!", options)
                logging.info("[workflow_end] End of deep seq data workflow.")
            else:
                # illegal step parameter specified: notify and exit
                logger.error("Illegal step parameter specified: '%s'" % step)
#                notify_admins('illegal_step')
                exit(1)

    def forbid(self):
        lock_file_name = "%s.forbid.lock" % self.run_dir

        try:
            if not self.lock_file_present(lock_file_name):
                if self.seq_complete():
                    if not self.dir_forbidden():
                        with open(lock_file_name, 'a'):
                            os.utime(lock_file_name)

                        time.sleep(10)
                        os.chmod(self.run_dir, 0)
        except:
            logging.exception("while forbidding access to deep seq data:")
#           notify_admins('forbid_dir', e)
            sys.exit(1)
        finally:
            if path.exists(lock_file_name):
                os.remove(lock_file_name) 
