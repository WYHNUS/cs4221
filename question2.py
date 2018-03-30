
import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

db_server = os.getenv("db_server")
db = os.getenv("db")
db_id = os.getenv("db_id")
db_pwd = os.getenv("db_pwd")

connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=" + db_server + ";Database=" + db + ";Uid=" + db_id + ";Pwd=" + db_pwd + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")

import random
for i in range(1,100001):
    branch_num = random.randint(1,20)
    balance = random.random()*100000
    insertion_str = "INSERT INTO account VALUES(" + str(i) + "," + str(branch_num) + "," + str(random.random()*100000) + ");"
    cursor.execute(insertion_str)
    cursor.commit()

count_str = "SELECT COUNT(*) FROM ACCOUNT"
cursor.execute(count_str)
print(cursor.fetchall())
cursor.commit()
