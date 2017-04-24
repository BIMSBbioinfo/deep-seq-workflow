class Conf

  HOSTNAME = `hostname`.chomp
  PREFIX = File.join(BASECALL_DIR, '.debug')
  # PREFIX = '/'
  DEBUG = true
  # DEBUG = false
  SEQ_DIR_REGEXP = '.seq_*'

  def self.global_conf
    unless DEBUG
      {
        hostname: HOSTNAME,
        log_dir:  File.join(PREFIX, 'data', 'basecalls', '.log'),
        basecall_dir: File.join(PREFIX, 'data', 'basecalls'),
        seq_dir_regexp: SEQ_DIR_REGEXP,
        safe_location_dir: File.join(PREFIX, 'data', 'bc_copy'),
        sample_sheets_dir: File.join(PREFIX, 'data', 'basecalls', 'sample_sheets'),
        restore_dir: File.join(PREFIX, 'data', 'basecalls', '.restore'),
        zib_archive_user: "bzpkuntz",
        zib_archive_host: "mdcbio.zib.de",
        zib_archive_dir: "/mdcbiosam/archiv/solexa",
        debug: DEBUG
      }
    else
      {
        hostname: HOSTNAME,
        log_dir: File.join(PREFIX, '.log'),
        basecall_dir: File.join(PREFIX, 'basecalls_test'), 
        seq_dir_regexp: SEQ_DIR_REGEXP,
        safe_location_dir: File.join(PREFIX, 'bc_copy_test'),
        sample_sheets_dir: File.join(PREFIX, 'sample_sheets_test'),
        restore_dir: File.join(PREFIX, 'restore_test'),
        zib_archive_user: "bzpkuntz",
        zib_archive_host: "mdcbio.zib.de",
        zib_archive_dir: "/mdcbiosam/archiv/solexa_gpg_test",
        debug: DEBUG
      }
    end

  end
end
