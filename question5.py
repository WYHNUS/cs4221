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
    start = time.clock()
    result = f(*args, **kwds)
    elapsed = time.clock() - start
    print "%s took %d time to finish" % (f.__name__, elapsed)
    return result
  return wrapper


def test(S,E,T,I):
    wrapped_sum_isolation = timed(sum_isolation)
    wrapped_run_exchanges = timed(run_exchanges)
    t1 = threading.Thread(target=wrapped_sum_isolation, args=(S,I))
    t2 = threading.Thread(target=wrapped_run_exchanges, args=(E, T, I))
    before_threads = time.clock()
    t2.start()
    t1.start()
    print(time.clock() - before_threads)

if __name__ == '__main__':
    print(sys.argv)
    #Expect S, E, T, I
    test(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])