import pyodbc
import sys
from sys import argv
import threading

from run_sums import sum_isolation
from run_exchanges import run_exchanges

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

db_server = os.getenv("db_server")
db = os.getenv("db")
db_id = os.getenv("db_id")
db_pwd = os.getenv("db_pwd")

def sum_thread(num_sum, isolation_level, result):
  result[0] = sum_isolation(num_sum, isolation_level)

def exchange_thread(num_exchange, num_thread, isolation_level, result):
  result[0] = run_exchanges(num_exchange, num_thread, isolation_level)

if __name__ == '__main__':
  prompt = "> "
  print("number of sums: ")
  num_sum = int(raw_input(prompt))
  print("number of exchanges in each thread: ")
  num_exchange = int(raw_input(prompt))
  print("number of threads: ")
  num_thread = int(raw_input(prompt))
  print("isolation level: ('read committed', 'repeatable read', 'serializable'): ")
  isolation_level = raw_input(prompt)
  if isolation_level.upper() not in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]:
    raise ValueError("Connection closed: invalid isolation level")

  connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=" + db_server + ";Database=" + db + ";Uid=" + db_id + ";Pwd=" + db_pwd + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
  cursor = connection.cursor()
  cursor.execute("SELECT SUM(balance) FROM account;")
  true_sum = cursor.fetchall()[0][0] 

  sums = sum_isolation(num_sum, isolation_level)
  result_sum = [None]
  result_exchange = [None]  
  t1 = threading.Thread(target=sum_thread, args=(num_sum, isolation_level, result_sum))
  t2 = threading.Thread(target=exchange_thread, args=(num_exchange, num_thread, isolation_level, result_exchange))
  t1.start()
  t2.start()
  t1.join()
  t2.join()

  correct_count = 0
  for val in result_sum[0]:
    if abs(val - true_sum) < 0.01:
      correct_count += 1
  accuracy = correct_count / float(num_sum) 
  print("correctness: " + str(accuracy))
