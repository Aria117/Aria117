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

    def read_excel_and_save(self):    
        sheet_name = "Target HIP"
        column_name = ["Year",
                       "SegNo",
                       "Target"]    
        df_raw_data = pd.read_excel(io=self.dir_file, sheet_name=sheet_name,names=column_name,dtype="str")

        # 删除重复行    
        df_cleaned = df_raw_data.drop_duplicates().reset_index(drop=True)

        # 获取文件信息
        file_name = self.dir_file.stem  
        creation_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_ctime)  
        last_modified_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_mtime)  
        print(f"文件名: {file_name}")  
        print(f"创建时间: {creation_time}")  
        print(f"最后修改时间: {last_modified_time}")  
        return df_cleaned 
    
    # 存入数据库
    def save_to_sql(self, table_name, last_modified_time_record=None):  
        df_cleaned = self.read_excel_and_save()  
        current_last_modified_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_mtime)  
  
        if last_modified_time_record is None or last_modified_time_record != current_last_modified_time:   
            try:    
                df_cleaned.to_sql(name=table_name, con=self.mssql_conn.sqlalchemy_connection(),  
                                  if_exists='replace', index=False)  
                print(f"数据已更新并替换到SQL Server的{table_name}表中")  
  
            except Exception as e:
                print(f"保存数据到SQL Server时出错: {e}")  
                sender = SendEmail(sender_name="Python Error",  
                                   sender_address=dict_email["HuangJinlong"],  
                                   receiver=[dict_email["HuangJinlong"]],  
                                   cc=[],  
                                   subject=f"Error saving data to SQL Server for {table_name}",  
                                   content=f"Failed to save data to SQL Server. \n {e}")  
                sender.send_email_with_text()

# 邮件发送者字典  
dict_email = {  
    "HuangJinlong": "daizha@schaeffler.com"  
}   

last_modified_time_record = None  
  
def job():  
    global last_modified_time_record  
    dir_file = r"\\schaeffler\taicang\Data\OP-SCA-PL\Public\01_HIP\Public Information.xlsx"  
    st_obj = ST(dir_file, sca_dev)  
    st_obj.save_to_sql("Target_HIP", last_modified_time_record)  
    # 更新全局变量以供下次检查  
    last_modified_time_record = datetime.datetime.fromtimestamp(Path(dir_file).stat().st_mtime)  

# 定点刷新 
schedule.every().day.at("9:00").do(job)  
while True:  
    schedule.run_pending()  
    time.sleep(1)