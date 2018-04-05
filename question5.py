import threading
from run_sums import sum_isolation
from run_exchanges import run_exchanges
import sys
import time
from functools import wraps

#timed wrapper says 0 time passes every time
def timed(f):
  @wraps(f)
  def wrapper(*args, **kwds):
    start = float(time.time())
    result = f(*args, **kwds)
    elapsed = float(time.time()) - float(start)
    print "%s took %f time to finish" % (f.__name__, elapsed)
    return result
  return wrapper

def test(S,E,T,I):
    print("S: %i, E: %i, T: %i, I: %s" % (S, E, T, I))
    wrapped_sum_isolation = timed(sum_isolation)
    wrapped_run_exchanges = timed(run_exchanges)
    t1 = threading.Thread(target=wrapped_sum_isolation, args=(S, I))
    t2 = threading.Thread(target=wrapped_run_exchanges, args=(E, T, I))
    t2.start()
    t1.start()
    t2.join()
    t1.join()

if __name__ == '__main__':
    print(sys.argv)
    #Expect S, E, T, I
    # test(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
    exchanges_tot = 2500
    sum_tot = 500
    test(sum_tot, exchanges_tot/1, 1, sys.argv[1])
    test(sum_tot, exchanges_tot/5, 5, sys.argv[1])
    test(sum_tot, exchanges_tot/10, 10, sys.argv[1])
    test(sum_tot, exchanges_tot/25, 25, sys.argv[1])
    test(sum_tot, exchanges_tot/50, 50, sys.argv[1])
