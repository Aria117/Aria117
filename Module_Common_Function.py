import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from pathlib import Path
import math
import sqlalchemy
import pandas as pd
import os

print("当前工作目录:", os.getcwd())


# define one object to send email
class SendEmail:
    """
    One object to send email
    """

    def __init__(self, sender_name, sender_address, receiver, cc, subject, content):
        """
        :param sender_name:  str | sender_name,
        :param sender_address:  str | sender_address,
        :param receiver: list | email address of receiver,
        :param cc: list | email address of Cc,
        :param subject: str | email title
        :param content: str | content of email, can only be normal text
        """
        self.sender_name = sender_name
        self.sender_address = sender_address
        self.receiver = receiver
        self.cc = cc
        self.subject = subject
        self.content = content

    def send_email_with_text(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        text = MIMEText(_text=self.content, _subtype="plain", _charset="utf-8")
        msg.attach(text)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()

    def send_email_with_html(self):
        # create connection to Email Serve
        email_server = smtplib.SMTP(host="mail-de-hza.schaeffler.com", port=25)

        # create email object
        msg = MIMEMultipart()

        # create subject
        title = Header(s=self.subject, charset="utf-8").encode()
        msg["Subject"] = title

        # set sender
        msg["From"] = formataddr((self.sender_name, self.sender_address))

        # set receiver
        msg["To"] = ",".join(self.receiver)

        # set Cc
        msg["Cc"] = ",".join(self.cc)

        # add content
        html = MIMEText(_text=self.content, _subtype="html", _charset="utf-8")
        msg.attach(html)

        # extend receiver list
        to_list = msg["To"].split(",")
        cc_list = msg["Cc"].split(",")
        to_list.extend(cc_list)

        # send email
        email_server.sendmail(from_addr=msg["From"], to_addrs=to_list, msg=msg.as_string())
        email_server.quit()
        
        
        
class MSSQL:
    """
    define one object to get connection with MS SQL Server
    """

    def __init__(self, server, user, password, database):
        """
        :param server: str | host name of MS SQL Server
        :param user: str | user to log in MS SQL Server
        :param password: str | password to log in MS SQL Server
        :param database: str | database name in MS SQL Server
        """
        self.server = server
        self.user = user
        self.database = database
        self.password = password

    def pyodbc_connection(self):
        """
        :return: pyodbc connection
        """
        if not self.database:
            raise (NameError, "Incorrect configuration of MS SQL Server !")

        # create connection to SQL Server
        connection_string = f'DRIVER={{SQL Server}};SERVER={self.server};DATABASE={self.database};UID={self.user};PWD={self.password}'
        try:
            pyodbc_con = pyodbc.connect(
                connection_string,
                fast_executemany=True
            )

            return pyodbc_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def sqlalchemy_connection(self):
        """
        :return: SQLAlchemy + pyodbc connection
        """

        # create connection to SQL Server
        try:
            engine = sqlalchemy.create_engine(
                'mssql+pyodbc://{}:{}@{}/{}?driver=ODBC+Driver+17+for+SQL+Server'.format(self.user,
                                                                                         self.password,
                                                                                         self.server,
                                                                                         self.database),
                fast_executemany=True)

            sqlalchemy_con = engine.connect()

            return sqlalchemy_con

        except Exception:
            raise Exception("Failed to connect to MS SQL Server !")

    def add_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_addextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def update_table_property(self, table_name, table_desc):
        """
        :param table_name: table name in MS SQL Server
        :return:
        """

        # create sql string
        sql = f"""
        EXEC sp_updateextendedproperty   
                @name = N'MS_Description',
                @value = N'{table_desc}',
                @level0type = N'Schema',
                @level0name = N'dbo',
                @level1type = N'Table',
                @level1name = N'{table_name}';
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql string
        cursor.execute(sql)

        # submit and close
        con.commit()
        con.close()

    def execute_sql_query(self, sql):
        """
        :param sql: sql query string
        :return: None
        """

        # get cursor
        con = self.pyodbc_connection()
        cursor = con.cursor()

        # execute sql query
        cursor.execute(sql)

        # commit and close
        con.commit()
        con.close()
