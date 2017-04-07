#!/usr/bin/env python

import os
import sys
import glob
import copy
import math
import time
import shutil
import yaml
import platform
import argparse
from pprint import pprint

import ana_daq_util as util
from job_manager import Jobs

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

DESCRIPTION='''
driver for ana daq prototype using hdf5 1.10 swmr/mwmr/virtual dataset
features
'''
EPILOG='''
'''


def getParser():
    global DESCRIPTION
    global EPILOG
    parser = argparse.ArgumentParser(description=DESCRIPTION,
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=EPILOG)
    parser.add_argument('--force', action='store_true',
                        help="overwrite existing files, clean out working dir")
    parser.add_argument('--rootdir', type=str, 
                        help="root directory for all runs")
    parser.add_argument('--rundir', type=str, 
                        help="run directory for this simulation")
    parser.add_argument('-c', '--config', type=str, default='config.yaml', 
                        help="config file")
    parser.add_argument('--verbose', type=int,
                        help='verbosity')
    parser.add_argument('--flush_interval', type=int,
                        help='how many events between flushes')
    parser.add_argument('--num_samples', type=int,
                        help='limit number of events')
    parser.add_argument('--kill', action='store_true',
                        help='kill any jobs in progress for the given prefix/config')
    parser.add_argument('--writer', action='store_true',
                        help='run one writer from local, for testing')
    parser.add_argument('--master', action='store_true',
                        help='run one writer from local, for testing. First run to get writer output.')
    parser.add_argument('--writers_hang', action='store_true',
                        help="have writers hang when done")
    parser.add_argument('--masters_hang', action='store_true',
                        help="have writers hang when done")
    return parser


def prepare_output_directory(config):
    rundir = os.path.join(config['rootdir'], config['rundir'])
    hdf5_path = os.path.join(rundir, 'hdf5')
    log_path =  os.path.join(rundir, 'logs')
    pid_path =  os.path.join(rundir, 'pids')
    results_path =  os.path.join(rundir, 'results')

    if os.path.exists(rundir):
        if config['force']:
            print("ana_daq_driver: Removing directory %s" % rundir)
            shutil.rmtree(rundir)
        else:
            print("FATAL: the directory %s exists, and --force was not given. "
                  "Use a different value for --rundir or pass --force to overwrite" % rundir)
            sys.exit(1)
            
    os.mkdir(rundir)
    os.mkdir(hdf5_path)
    os.mkdir(log_path)
    os.mkdir(results_path)
    os.mkdir(pid_path)
    
    if config['lfs']['do_stripe']:
        stripe_size_mb = config['lfs']['stripe_size_mb']
        OST_start_index = config['lfs']['OST_start_index']
        count = config['lfs']['count']
        assert isinstance(stripe_size_mb, int)
        assert stripe_size_mb>=1 and stripe_size_mb < 1024
        stripe_size = stripe_size_mb << 20
        cmd = 'lfs setstripe --stripe-size %d --index %d --count %d %s' % \
              (stripe_size, OST_start_index, count, hdf5_path)
        print("striping hdf5 subdir:\n%s" % cmd)
        assert 0 == os.system(cmd)


def assign_hosts(group, config):
    cfg = config[group]
    if cfg['num'] == 0:
        return []
    all_hosts = copy.deepcopy(config[group]['hosts'])
    hosts = [all_hosts[idx//cfg['num_per_host']] for idx in range(cfg['num'])]
    return hosts

  
def copy_config_to_rundir(config):
    config_filename = os.path.join(config['rootdir'], config['rundir'], 'config.yaml')
    assert config['force'] or not os.path.exists(config_file)
    with open(config_filename,'w') as file:
        file.write(yaml.dump(config, Dumper=Dumper))
    return config_filename


def run(argv):
    parser = getParser()
    args = parser.parse_args(argv[1:])
    config = yaml.load(open(args.config,'r'))
    for ky in ['force','rootdir','rundir','verbose','flush_interval',
               'writers_hang', 'masters_hang', 'num_samples']:
        val = getattr(args,ky)
        if val in [None, False]: continue
        print("replacing config[%s] with %s (from command line)" % (ky,val))
        config[ky]=val
    
    jobs = Jobs(config)

    if args.kill:
        jobs.kill_all()
        return

    prepare_output_directory(config)
    config_filename = copy_config_to_rundir(config)

    daq_writer_hosts = assign_hosts('daq_writer', config)
    daq_master_hosts = assign_hosts('daq_master', config)
    ana_reader_hosts = assign_hosts('daq_master', config)

    jobs.launch('daq_writer', daq_writer_hosts)
    time.sleep(2)
    jobs.launch('daq_master', daq_master_hosts)
    time.sleep(2)
    jobs.launch('ana_reader_master', ana_reader_hosts)
    jobs.wait()

if __name__ == '__main__':
    run(sys.argv)
    
