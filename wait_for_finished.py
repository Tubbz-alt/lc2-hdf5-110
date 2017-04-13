import os
import time
import sys

assert (len(sys.argv))>1
sys.argv.pop(0)
loc = sys.argv.pop(0)

while len(sys.argv):
    fname = os.path.join(loc, 'logs', sys.argv.pop(0))
    while (not os.path.exists(fname)):
        time.sleep(1)
