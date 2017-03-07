import h5py
import numpy as np
import os
import sys
import time

def run_command(command):
    t0 = time.time()
    print("-----------")
    print(command)
    res = os.system(command)
    print(" -- finished in %.2f sec" % (time.time()-t0,))
    assert res == 0, "command failed"

if __name__ == '__main__':
    assert len(sys.argv)==2, "usage: driver.py N where N is how many elements in src dsets"
    N = int(sys.argv[1])

    t0 = time.time()
    ones = np.ones(N).astype(np.int16)
    twos = 2*np.ones(2*N).astype(np.int16)
    ones_with_fill = np.ones(2*N).astype(np.int16)
    ones_with_fill[N:]=-1
    
    srcA = h5py.File('srcA.h5','w')
    srcA['dsetA'] = ones
    srcA.close()

    srcB = h5py.File('srcB.h5','w')
    srcB['dsetB'] = twos
    srcB.close()
    print("created files in %.2f sec" % (time.time()-t0,))
    run_command('h5c++ vds_unlimited.cpp -o vds_unlimited')
    run_command('./vds_unlimited %d' % N)

    t0 = time.time()
    master = h5py.File('master.h5','r')
    vds_data = master['vds'][:]

    np.testing.assert_equal(vds_data[0::2], ones_with_fill)
    np.testing.assert_equal(vds_data[1::2], twos)
    print("read/tested vds in %.2f sec" % (time.time()-t0,))
    sys.stdout.flush()
    os.system('ls -lrth')

