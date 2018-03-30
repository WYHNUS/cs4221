import pyodbc
import random
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

def exchange():
  cursor = connection.cursor()
  # pick random different account number
  a1 = random.randint(1, 100000)
  a2 = random.randint(1, 100000)
  while a2 == a1:
    a2 = random.randint(1, 100000)
  # read from account A1
  cursor.execute("SELECT balance FROM account WHERE account_number=" + str(a1))
  for row in cursor.fetchall():
    balance_a1 = row[0]
  # read from account A2
  cursor.execute("SELECT balance FROM account WHERE account_number=" + str(a2))
  for row in cursor.fetchall():
    balance_a2 = row[0]
  # swap balance between A1 and A2 
  cursor.execute("UPDATE account set balance = " + str(balance_a2) + " WHERE account_number=" + str(a1))
  cursor.commit()
  cursor.execute("UPDATE account set balance = " + str(balance_a1) + " WHERE account_number=" + str(a2))
  cursor.commit()

#def run_exchange(num_exchange, num_thread, isolation_level):
  
exchange()
