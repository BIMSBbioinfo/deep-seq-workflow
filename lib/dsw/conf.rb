class Conf

  HOSTNAME = `hostname`.chomp
  # PREFIX = FileUtils.pwd
  PREFIX = '/'
  BASECALL_DIR = File.join(PREFIX, 'data', 'basecalls')
  SEQ_DIR_REGEXP = '.seq_*'
  SAFE_LOCATION_DIR = File.join(PREFIX, 'data', 'bc_copy')
  SAMPLE_SHEETS_DIR = File.join(PREFIX, 'data', 'basecalls', 'sample_sheets')
  RESTORE_DIR = File.join(PREFIX, 'data', 'basecalls', '.restore')
  DEBUG = true

  ZIB_ARCHIVE_USER = "bzpkuntz"
  ZIB_ARCHIVE_HOST = "mdcbio.zib.de"
  ZIB_ARCHIVE_DIR  = "/mdcbiosam/archiv/solexa"

  def self.global_conf
    unless DEBUG
      {
        hostname: HOSTNAME,
        prefix: PREFIX,
        basecall_dir: BASECALL_DIR,
        seq_dir_regexp: SEQ_DIR_REGEXP,
        safe_location_dir: SAFE_LOCATION_DIR,
        sample_sheets_dir: SAMPLE_SHEETS_DIR,
        restore_dir: RESTORE_DIR,
        zib_archive_user: ZIB_ARCHIVE_USER,
        zib_archive_host: ZIB_ARCHIVE_HOST, 
        zib_archive_dir: ZIB_ARCHIVE_DIR,
        debug: DEBUG
      }
    else
      {
        hostname: HOSTNAME,
        prefix: File.join(BASECALL_DIR, '.debug'),
        basecall_dir: File.join(PREFIX, 'basecalls_test'), 
        seq_dir_regexp: SEQ_DIR_REGEXP,
        safe_location_dir: File.join(PREFIX, 'bc_copy_test'),
        sample_sheets_dir: File.join(PREFIX, 'sample_sheets_test'),
        restore_dir: File.join(PREFIX, 'restore_test'),
        zib_archive_user: ZIB_ARCHIVE_USER,
        zib_archive_host: ZIB_ARCHIVE_HOST,
        zib_archive_dir: "/mdcbiosam/archiv/solexa_gpg_test",
        debug: DEBUG
      }
    end
  end
end
