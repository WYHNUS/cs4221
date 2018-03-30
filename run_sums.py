# Only one of the isolation level options can be set at a time, and it remains set for that connection until it is explicitly changed. All read operations performed within the transaction operate under the rules for the specified isolation level unless a table hint in the FROM clause of a statement specifies different locking or versioning behavior for a table.

#  When you change a transaction from one isolation level to another, resources that are read after the change are protected according to the rules of the new level. Resources that are read before the change continue to be protected according to the rules of the previous level. For example, if a transaction changed from READ COMMITTED to SERIALIZABLE, the shared locks acquired after the change are now held until the end of the transaction.

# If you issue SET TRANSACTION ISOLATION LEVEL in a stored procedure or trigger, when the object returns control the isolation level is reset to the level in effect when the object was invoked. For example, if you set REPEATABLE READ in a batch, and the batch then calls a stored procedure that sets the isolation level to SERIALIZABLE, the isolation level setting reverts to REPEATABLE READ when the stored procedure returns control to the batch.

import pyodbc
import sys
import os
from os.path import join, dirname
from dotenv import load_dotenv

def sum_isolation(S, I):
    res = []
    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    db_server = os.getenv("db_server")
    db = os.getenv("db")
    db_id = os.getenv("db_id")
    db_pwd = os.getenv("db_pwd")

    connection = pyodbc.connect("Driver={ODBC Driver 13 for SQL Server};Server=" + db_server + ";Database=" + db + ";Uid=" + db_id + ";Pwd=" + db_pwd + ";Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
    cursor = connection.cursor()

    if I.upper() not in ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SNAPSHOT", "SERIALIZABLE"]:
        connection.close()
        raise ValueError("Connection closed. Isolation level not one of READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SNAPSHOT, SERIALIZABLE")

    isolation_str = "SET TRANSACTION ISOLATION LEVEL " + I
    cursor.execute(isolation_str)
    cursor.commit()

    sum_str = "Select sum(balance) from account;"
    for i in range(S):
        cursor.execute(sum_str)
        total = cursor.fetchall()[0][0]
        res.append(total)

    connection.close()
    return res
        
if __name__ == '__main__':
    print(sys.argv)
    result = sum_isolation(int(sys.argv[1]), sys.argv[2])
    print(result)
