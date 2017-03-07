import h5py
import numpy as np
import os
import sys
import time
import argparse

def run_command(command):
    t0 = time.time()
    res = os.system(command)
    seconds = time.time()-t0
    assert res == 0, "command failed"
    print("tm=%.2fsec command=%s" % (seconds, command))
    
DESCRIPTION='''
Try VDS and SWMR
'''

EPILOG='''
'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=EPILOG)
    parser.add_argument('-d', '--delay', type=int, help="average delay in microseconds between writes, for each writer", default=1000000)
    parser.add_argument('-f', '--flush', type=int, help="number of writes between flushes", default=1)
    parser.add_argument('-n', '--num', type=int, help="approximate number of writes each writer does", default=30)
    parser.add_argument('-w', '--writers', type=int, help="number of writers", default=2)
    parser.add_argument('-o', '--output', type=str, help="outputfile for writers, include %d for writer number", default="/reg/d/ana01/temp/davidsch/lc2/vds_swmr/writer-%d.h5")
    parser.add_argument('-m', '--master', type=str, help="outputfile for master", default="/reg/d/ana01/temp/davidsch/lc2/vds_swmr/master.h5")
    args = parser.parse_args()

    run_command('h5c++ writer.cpp -o writer')
    run_command('h5c++ master.cpp -o master')

    writer2num = {}
    run_command('./master %d %s %s' % (args.writers, args.output, args.master))
    for writer in range(args.writers):
        writer2num[writer] = args.num + writer
        outputfile = args.output % writer
        if os.path.exists(outputfile): os.unlink(outputfile)
        run_command('./writer %d %d %d %d %s &' % (writer, args.delay, args.flush, writer2num[writer], outputfile))
    run_command('h5watch %s/vds' % args.master)


