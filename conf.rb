require 'rsync'
require 'childprocess'
require 'fileutils'
require 'find'
require 'logger'
require 'net/smtp'
require 'yaml'
require 'shellwords'

ChildProcess.posix_spawn = true

class Conf

  HOSTNAME = `hostname`.chomp
  # PREFIX = FileUtils.pwd
  PREFIX = '/'
  BASECALL_DIR = File.join(PREFIX, 'data', 'basecalls')
  SEQ_DIR_REGEXP = '.seq_*'
  SAFE_LOCATION_DIR = File.join(PREFIX, 'data', 'bc_copy')
  SAMPLE_SHEETS_DIR = File.join(PREFIX, 'data', 'basecalls', 'sample_sheets')
  DEBUG = false

  def self.global_conf
    {
     hostname: HOSTNAME,
     prefix: PREFIX,
     basecall_dir: BASECALL_DIR,
     seq_dir_regexp: SEQ_DIR_REGEXP,
     safe_location_dir: SAFE_LOCATION_DIR,
     sample_sheets_dir: SAMPLE_SHEETS_DIR,
     debug: DEBUG
    }
  end
end
