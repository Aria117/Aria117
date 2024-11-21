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
        self.last_modified_time_record = None  # 初始化属性  
  
    def read_excel_and_save(self):  
        sheet_name = "HP3010"  
        column_name = ["Business_division", 
                       "Business_unit", 
                       "BU", 
                       "Plant_harm", 
                       "PH", 
                       "Intercomp/Commercial",  
                       "Main_segment", 
                       "MS", 
                       "Customer_harm", 
                       "Customer_No", 
                       "Material_harm", 
                       "SAP_No",  
                       "MRP_controller", 
                       "MRP_No", 
                       "Measurement_day", 
                       "GI_date", 
                       "Goods_issue_current",  
                       "Min_date_tol_GI", 
                       "Max_date_tol_GI", 
                       "Min_quantity_tol_GI", 
                       "Max_quantity_tol_GI",  
                       "Max tol plan date GI", 
                       "Ship_to_party", 
                       "NO", 
                       "Sales_document", 
                       "Item_order",  
                       "Schedule_line", 
                       "Delivery", 
                       "Relevant_GI", 
                       "Delivery_reliability_GI", 
                       "Target_quantity_GI",  
                       "Delivered_quantity_per_schedule_line_and_delivery_item_BME", 
                       "Transportation_quantity"]  
        df_raw_data = pd.read_excel(io=self.dir_file, sheet_name=sheet_name, names=column_name, dtype="str", skiprows=2)  
        df_cleaned = df_raw_data.iloc[3:].reset_index(drop=True).drop_duplicates().reset_index(drop=True).dropna(how="all")    
        return df_cleaned  
  
    def save_to_sql(self, table_name):  
        df_cleaned = self.read_excel_and_save()  
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
        print(f"文件名: {file_name}\n创建时间: {creation_time}\n最后修改时间: {last_modified_time}")  
  
def job():  
    dir_file = r"C:\Users\DAIZHA\Documents\HIP_3010_GI 2024.xlsx"  
    st_instance = ST(dir_file, sca_dev)  
    st_instance.print_file_info()  
    st_instance.save_to_sql("HP3010")  
  
# 定点刷新  
schedule.every().minute.do(job)  
while True:  
    schedule.run_pending()  
    time.sleep(1)