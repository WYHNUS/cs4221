import pyodbc
import random
from multiprocessing.dummy import Pool
from sys import argv
import time

import os
from os.path import join, dirname
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

db_server = os.getenv("db_server")
db = os.getenv("db")
db_id = os.getenv("db_id")
db_pwd = os.getenv("db_pwd")

def exchange(connection):
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

def thread_exchange(param):
  num_exchange = param[0]
  isolation_level = param[1]
  start_time = time.time()
  connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=" + db_server + ";Database=" + db + ";Uid=" + db_id + ";Pwd=" + db_pwd + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
  isolation_str = "SET TRANSACTION ISOLATION LEVEL " + isolation_level
  connection.execute(isolation_str)
  connection.commit()
  for i in range(num_exchange):
    exchange(connection)
    time.sleep(0.0001)
  end_time = time.time()
  return end_time - start_time

def run_exchanges(num_exchange, num_thread, isolation_level): 
  pool = Pool(num_thread)
  param = [(num_exchange, isolation_level)] * num_thread
  results = pool.map(thread_exchange, param)
  return results

if __name__ == '__main__':
  prompt = ">>>>>>>>> "
  print("number of exchanges in each thread: ")
  num_exchange = int(raw_input(prompt))
  print("number of threads: ")
  num_thread = int(raw_input(prompt))
  print("isolation level: ('read committed', 'repeatable read', 'serializable'): ")
  isolation_level = raw_input(prompt)
  if isolation_level.upper() not in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"]:
     raise ValueError("Connection closed: invalid isolation level")

  result = run_exchanges(num_exchange, num_thread, isolation_level)
  print(result)

