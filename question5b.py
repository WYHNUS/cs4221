import pyodbc
import threading
from run_sums import sum_isolation
from run_exchanges import run_exchanges
import sys

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

def exchange_thread(num_exchange, num_thread, isolation_level):
  run_exchanges(num_exchange, num_thread, isolation_level)

def test(S, E, T, I):
    print("S: %i, E: %i, T: %i, I: %s" % (S, E, T, I))

    connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=" + db_server + ";Database=" + db + ";Uid=" + db_id + ";Pwd=" + db_pwd + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = connection.cursor()
    cursor.execute("SELECT SUM(balance) FROM account;")
    true_sum = cursor.fetchall()[0][0]


    result = [None]
    t1 = threading.Thread(target=sum_thread, args=(S, I, result))
    t2 = threading.Thread(target=exchange_thread, args=(E, T, I))
    t2.start()
    t1.start()
    t2.join()
    t1.join()

    correct_count = 0
    for val in result[0]:
      if abs(val - true_sum) < 0.01:
        correct_count += 1
    accuracy = correct_count / float(S)
    print("correctness: " + str(accuracy))

if __name__ == '__main__':
    print(sys.argv)
    exchanges_tot = 2500
    sum_tot = 500
    test(sum_tot, exchanges_tot/1, 1, sys.argv[1])
    test(sum_tot, exchanges_tot/5, 5, sys.argv[1])
    test(sum_tot, exchanges_tot/10, 10, sys.argv[1])
    test(sum_tot, exchanges_tot/25, 25, sys.argv[1])
    test(sum_tot, exchanges_tot/50, 50, sys.argv[1])
