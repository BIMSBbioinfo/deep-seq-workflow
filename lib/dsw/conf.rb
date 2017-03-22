class Conf

  HOSTNAME = `hostname`.chomp
  # PREFIX = FileUtils.pwd
  PREFIX = '/'
  BASECALL_DIR = File.join(PREFIX, 'data', 'basecalls')
  SEQ_DIR_REGEXP = '.seq_*'
  SAFE_LOCATION_DIR = File.join(PREFIX, 'data', 'bc_copy')
  SAMPLE_SHEETS_DIR = File.join(PREFIX, 'data', 'basecalls', 'sample_sheets')
  DEBUG = false

  ZIB_ARCHIVE_USER = "bzpkuntz"
  ZIB_ARCHIVE_HOST = "mdcbio.zib.de"
  ZIB_ARCHIVE_DIR  = "/mdcbiosam/archiv/solexa"

  def self.global_conf
    {
      hostname: HOSTNAME,
      prefix: PREFIX,
      basecall_dir: BASECALL_DIR,
      seq_dir_regexp: SEQ_DIR_REGEXP,
      safe_location_dir: SAFE_LOCATION_DIR,
      sample_sheets_dir: SAMPLE_SHEETS_DIR,
      zib_archive_user: ZIB_ARCHIVE_USER,
      zib_archive_host: ZIB_ARCHIVE_HOST, 
      zib_archive_dir: ZIB_ARCHIVE_DIR,
      debug: DEBUG
    }
  end
end
