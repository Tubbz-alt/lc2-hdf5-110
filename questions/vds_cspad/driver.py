import os
import sys
import numpy as np
import h5py

def driver(N,M):
    srcs = [1,2]
    fnames = []
    for src in srcs:
        fname = "cspad_src_%d.h5" % src
        h5=h5py.File(fname, "w")
        fnames.append(fname)
        data = np.zeros((N,M,M), dtype=np.int16)
        data[:] = src
        h5['cspad'] = data
        h5.close()

    assert 0 == os.system('h5c++ master.cpp -o master')
    assert 0 == os.system('./master %d %s' % (M, ' '.join(fnames)))
        
if __name__ == '__main__':
    assert len(sys.argv)==3, "give an N and M"
    N = int(sys.argv[1])
    M = int(sys.argv[2])
    
    driver(N,M)
