#!/usr/bin/env python2.7

import logging
import smtplib
import subprocess
from email.mime.text import MIMEText

SENDMAIL_PATH = "/usr/sbin/sendmail"

class EmailHandler(logging.Handler):
    def __init__(self, toaddr, fromaddr):
        super(EmailHandler, self).__init__()
        self.toaddr = toaddr
        self.fromaddr = fromaddr
        self.body = ""

    def emit(self, record):
        self.acquire()
        try:
            self.body += "{}: {}: {}\n".format(record.name, record.levelname, record.getMessage())
        finally:
            self.release()

    def finalize(self):
        m = MIMEText(self.body)
        m["Subject"] = "backupmgr: backup results"
        m["From"] = self.fromaddr
        m["To"] = self.toaddr

        argv = [SENDMAIL_PATH, self.toaddr]
        proc = subprocess.Popen(argv, stdin=subprocess.PIPE)

        proc.stdin.write(m.as_string())
        proc.stdin.close()
