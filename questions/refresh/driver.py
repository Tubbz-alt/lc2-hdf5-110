import h5py
import numpy as np
import os
import sys
import time

usage = '''
driver.py N M C outputfile
    where N = total number of elements to write
          M = number of datasets to write
          C = chunksize
          outputfile = name of output file

    for example, 
    
    python driver.py 100 10 100 /reg/d/ana01/temp/davidsch/lc2/refresh.h5 
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
    num_elem = int(sys.argv[1])
    num_datasets = int(sys.argv[2])
    chunk_size = min(num_elem, int(sys.argv[3]))
    outputfile = sys.argv[4]
    
    h5 = h5py.File(outputfile, 'w', libver='latest')
    for dsetname in range(num_datasets):
        dset=h5.create_dataset("%5.5d" % dsetname, (0,), dtype='i8', chunks=(chunk_size,), maxshape=(None,))
        dset[0:num_elem] = np.arange(num_elem, dtype=np.int64)
    h5.close()
                     
    t0 = time.time()
    run_command('h5c++ reader.cpp -o reader')
    print("compiled reader in %.2f sec" % (time.time()-t0,))

    run_command('./reader %d %d 1 %s' % (num_elem, num_datasets, outputfile))


