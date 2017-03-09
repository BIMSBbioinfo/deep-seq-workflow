class Mailer
  EVERYBODY = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de", "quedenau@mdc-berlin.de", "madlen.sohn@mdc-berlin.de"]
  ADMINS = ["carlomaria.massimo@mdc-berlin.de", "dan.munteanu@mdc-berlin.de"]

  def notify_admins(op, error =nil)
    unless DEBUG
      ADMINS.each do |adm|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{adm}
Subject: [Deep Seq workflow] Error: #{op}
Date: #{Time.now}

Error code: #{op}
Run dir: #{@new_run_dir.nil? ? @run_dir : @new_run_dir}
Host: #{HOSTNAME}

See #{@log_file_name} for details.\n|

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
      logger.debug "notify_admins(#{op})"
      logger.debug error.class
    end
  end

  def notify_run_finished
    unless DEBUG
      users = EVERYBODY
      users.each do |user|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{user}
Subject: [Deep Seq workflow] Processing of run #{@run_name} finished
Date: #{Time.now}

Run dir: #{@new_run_dir.nil? ? @run_dir : @new_run_dir}
Access for the users has been restored.
Demultiplexing may still be underway.

---\ndsw\n|

        Net::SMTP.start('localhost', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', user
        end
      end
    else
      logger.debug "Processing of run #{@run_name} finished"
    end

  end

  def notify_run_error(recipients)
    unless DEBUG
      recipients.each do |rcp|
        msg = %Q|From: deep_seq_workflow <dsw@mdc-berlin.net>
To: #{rcp}
Subject: [Deep Seq workflow] Errors during sequencing run detected
Date: #{Time.now}

During run:

#{@run_name}

errors were detected by the sequencing software; please check the log files:

#{@new_run_dir.nil? ? @run_dir : @new_run_dir}/Logs/Error_*.log

and contact the vendor if needed.

---\ndsw|

        Net::SMTP.start('localhost', 25) do |smtp|
          smtp.send_message msg, 'dsw@mdc-berlin.net', rcp
        end
      end
    else
      logger.debug "Run error detected"
    end
  end

end
