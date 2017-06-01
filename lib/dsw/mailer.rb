class Mailer
  EVERYBODY = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de", "quedenau@mdc-berlin.de", "madlen.sohn@mdc-berlin.de", "kirsten.richter@mdc-berlin.de"]
  ADMINS = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de"]

  def self.notify_admins(manager, op_code, error =nil)
    unless Conf.global_conf[:debug]
      ADMINS.each do |adm|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{adm}
Subject: [Deep Seq workflow] Error: #{op_code}
Date: #{Time.now}

Error code: #{op_code}
Run dir: #{manager.new_run_dir.nil? ? manager.run_dir : manager.new_run_dir}
Host: #{HOSTNAME}

See #{manager.log_file_name} for details.\n|

        unless error.nil?
          msg << "\n"
          msg << error.message
          msg << "\n"
          msg << error.backtrace.join("\n")
          msg << "\n"
        end

        msg << "\n---\ndsw"

        Net::SMTP.start('localhost', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', adm
        end
      end
    else
      manager.logger.debug "notify_admins(#{op_code})"
      manager.logger.debug error.class
    end
  end

  def self.notify_run_finished(manager)
    unless Conf.global_conf[:debug]
      users = EVERYBODY
      users.each do |user|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{user}
Subject: [Deep Seq workflow] Processing of run #{manager.run_name} finished
Date: #{Time.now}

Run dir: #{manager.new_run_dir.nil? ? manager.run_dir : manager.new_run_dir}
Access for the users has been restored.
The backup procedure and demultiplexing may still be underway.

---\ndsw\n|

        Net::SMTP.start('localhost', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', user
        end
      end
    else
      manager.logger.debug "Processing of run #{manager.run_name} finished"
    end

  end

  def self.notify_run_errors(manager, recipients)
    unless Conf.global_conf[:debug]
      recipients.each do |rcp|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{rcp}
Subject: [Deep Seq workflow] Errors during sequencing run detected
Date: #{Time.now}

During run:

#{manager.run_name}

errors were detected by the sequencing software; please check the log files:

#{manager.new_run_dir.nil? ? manager.run_dir : manager.new_run_dir}/Logs/Error_*.log

and contact the vendor if needed.

---\ndsw|

        Net::SMTP.start('localhost', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', rcp
        end
      end
    else
      manager.logger.debug "Run error detected"
    end
  end

end