import pandas as pd  
from pathlib import Path  
import re  
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
  
    def read_excel_and_split(self):  
        sheet_name = "Sheet1"
        df = pd.read_excel(self.dir_file, sheet_name=sheet_name)

        # 定义固定的列名和日期列  
        fixed_cols = ['Total', 'Total_without_stock']  
        date_cols = [col for col in df.columns if re.match(r'M \d\d/\d{4}', col)]  
  
        # 删除重复行  
        df.dropna(subset=["Material"], how='any', inplace=True)  
        df.drop_duplicates(inplace=True)  
        df.reset_index(drop=True, inplace=True)  
  
        # 合并数据  
        merged_df = pd.DataFrame()  
        for date_col in date_cols:  
            non_date_cols = [col for col in df.columns if col not in date_cols]  
            year_month = re.sub(r'M (\d\d)/(\d{4})', r'\2-\1', date_col)  
            temp_df = df[non_date_cols].copy()  
            temp_df.insert(0, 'Date', year_month)  
            merged_df = pd.concat([merged_df, temp_df], ignore_index=True)

        file_name = self.dir_file.stem  
        creation_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_ctime)  
        last_modified_time = datetime.datetime.fromtimestamp(self.dir_file.stat().st_mtime)
        print(f"文件名: {file_name}")  
        print(f"创建时间: {creation_time}")  
        print(f"最后修改时间: {last_modified_time}")
  
        return merged_df 

    # 存入数据库
    def save_to_sql(self, table_name, last_modified_time_record=None):    
        df_cleaned = self.read_excel_and_split()  
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
    dir_file = r"\\schaeffler.com\Taicang\Data\OP-SCA-P3\P32S\08_Summary\15_Segment_IE\冲压集成件需求匹配监控\钢带集成变更_20240701.xlsx"  
    st_obj = ST(dir_file, sca_dev)  
    st_obj.save_to_sql("Steel_belt_merged_date", last_modified_time_record)  
    # 更新全局变量以供下次检查  
    last_modified_time_record = datetime.datetime.fromtimestamp(Path(dir_file).stat().st_mtime)  

# 定点刷新 
schedule.every().minute.do(job)  
while True:  
    schedule.run_pending()  
    time.sleep(1)