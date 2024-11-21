import pandas as pd  
from pathlib import Path  
import datetime  
from Module_Common_Function import SendEmail, MSSQL  
import schedule  
import time  
  
# 创建与SQL Server的连接  
sca_dev = MSSQL(server="WS007238",  
                database="SCA_Digital_Dev",  
                user="SCA_Admin",  
                password="JAdmin!2309!")  
  
class ST:  
    def __init__(self, dir_file, mssql_conn):  
        self.dir_file = Path(dir_file)  
        self.mssql_conn = mssql_conn  
        self.last_modified_time_record = None  
  
    def read_excel_and_clean(self):  
        sheet_name = "HP3001"  
        column_name = ["Business_division",
                       "Business_unit", 
                       "BU", 
                       "Plant_harm",
                       "PH", 
                       "Intercomp/Commercial",  
                       "Main_segment", 
                       "MS",
                       "Customer_harm", 
                       "Customer_no", 
                       "Material_harm", 
                       "SAP_no",  
                       "MRP_controller", 
                       "MRP_no", 
                       "Measurement_day", 
                       "Sales_doc_type", 
                       "SDT", 
                       "Percentage", 
                       "GI_no"]  
        df_raw_data = pd.read_excel(io=self.dir_file, sheet_name=sheet_name, names=column_name, header=3, usecols='A:S', dtype="str")  
        df_cleaned = df_raw_data.drop_duplicates().dropna(how="all").reset_index(drop=True)  
        return df_cleaned  
  
    def save_to_sql(self, table_name):  
        df_cleaned = self.read_excel_and_clean()  
        current_last_modified_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_mtime)  
  
        if self.last_modified_time_record is None or self.last_modified_time_record != current_last_modified_time:  
            try:  
                df_cleaned.to_sql(name=table_name, con=self.mssql_conn.sqlalchemy_connection(), if_exists='replace', index=False)  
                print(f"数据已更新并替换到SQL Server的{table_name}表中")  
                self.last_modified_time_record = current_last_modified_time  
            except Exception as e:  
                print(f"保存数据到SQL Server时出错: {e}")  
                self.send_error_email(table_name, e)  
  
    def send_error_email(self, table_name, error):  
        sender = SendEmail(sender_name="Python Error",  
                           sender_address="daizha@schaeffler.com",  
                           receiver=["daizha@schaeffler.com"],  
                           cc=[],  
                           subject=f"Error saving data to SQL Server for {table_name}",  
                           content=f"Failed to save data to SQL Server. \n {error}")  
        sender.send_email_with_text()  
  
    def print_file_info(self):  
        file_name = self.dir_file.stem  
        creation_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_ctime)  
        last_modified_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_mtime)  
        print(f"文件名: {file_name}")  
        print(f"创建时间: {creation_time}")  
        print(f"最后修改时间: {last_modified_time}")  
  
def job():  
    dir_file = r"C:\Users\DAIZHA\Documents\HIP_3010_GI 2024.xlsx"  
    st_instance = ST(dir_file, sca_dev)  
    st_instance.print_file_info()  
    st_instance.save_to_sql("HP3001")  
  
# 定点刷新  
schedule.every().day.at("9:00").do(job)  
while True:  
    schedule.run_pending()  
    time.sleep(1)