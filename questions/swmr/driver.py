import h5py
import numpy as np
import os
import sys
import time

usage = '''
driver.py D F N outputfile
    where D = delay in microseconds between writes
          F = number of writes between flushes and chunksize
          N = total number of elements to write
          outputfile = name of output file
'''

def run_command(command):
    t0 = time.time()
    print("-----------")
    print(command)
    res = os.system(command)
    print(" -- finished in %.2f sec" % (time.time()-t0,))
    assert res == 0, "command failed"

if __name__ == '__main__':
    assert len(sys.argv)==5, usage
    delay = int(sys.argv[1])
    flush = int(sys.argv[2])
    num = int(sys.argv[3])
    outputfile = sys.argv[4]

    t0 = time.time()
    run_command('h5c++ writer.cpp -o writer')
    print("compiled writer in %.2f sec" % (time.time()-t0,))

    run_command('./writer %d %d %d %s &' % (delay, flush, num, outputfile))
    run_command('h5watch --dim %s/data' % outputfile)


