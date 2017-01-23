#!/usr/bin/env python

import os
import sys
import copy
import math
import time
import shutil
import yaml
import platform
import paramiko as pm
import argparse
from pprint import pprint

import ana_daq_util as util
from job_manager import Jobs

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
    parser.add_argument('-c', '--config', type=str, 
                        help="config file")
    parser.add_argument('--verbose', type=int,
                        help='verbosity')
    parser.add_argument('--flush_interval', type=int,
                        help='how many events between flushes')
    parser.add_argument('--time', type=int, 
                        help='number of seconds to run for, kill after that.')
    parser.add_argument('--kill', action='store_true',
                        help='kill any jobs in progress for the given prefix/config')
    parser.add_argument('--writer', action='store_true',
                        help='run one writer from local, for testing')
    parser.add_argument('--writers_hang', action='store_true',
                        help="have writers hang when done")
    return parser


def divide_datasets_between_writers(config):
    '''determines start/stop/stride and which datasets for each type each writer does

    For a given dataset type, ie, 'small', 'vlen', 'detector', 
    if there are more datasets than writers, the datasets are divided among the writers
    (controls style)

    If there are less datasets then writers, the writing is round-robined among the 
    writers (DAQ style)

    For example, if the config looks like:

    daq_writers:
      num: 3
      datasets:
        small:
          num: 10
          shots_per_sample: 1
        vlen:
          num: 3
          shots_per_sample: 1
        detector:
          num: 1
         shots_per_sample: 100

    Then the writers output will be

    [{'detector': {'first_dset': 0, 'num_dsets': 1, 'start': 0, 'stride': 300},
    'small': {'first_dset': 0, 'num_dsets': 4, 'start': 0, 'stride': 1},
    'vlen': {'first_dset': 0, 'num_dsets': 1, 'start': 0, 'stride': 1}},
    {'detector': {'first_dset': 0, 'num_dsets': 1, 'start': 100, 'stride': 300},
    'small': {'first_dset': 4, 'num_dsets': 3, 'start': 0, 'stride': 1},
    'vlen': {'first_dset': 1, 'num_dsets': 1, 'start': 0, 'stride': 1}},
    {'detector': {'first_dset': 0, 'num_dsets': 1, 'start': 200, 'stride': 300},
    'small': {'first_dset': 7, 'num_dsets': 3, 'start': 0, 'stride': 1},
    'vlen': {'first_dset': 2, 'num_dsets': 1, 'start': 0, 'stride': 1}}]
    '''
    cfg = config['daq_writers']
    num_writers = cfg['num']
    
    dset_types = ['small','vlen','detector']
    writers = [{'small':{}, 'vlen':{}, 'detector':{}} for idx in range(num_writers)]

    for dset_type in dset_types:
        dset_type_cfg = cfg['datasets'][dset_type]
        num_dsets = dset_type_cfg['num']
        if num_dsets == 0:
            continue
        if num_dsets >= num_writers:
            offsets, counts = util.divide_evenly(num_dsets, num_writers)
            for writer in range(num_writers):
                params = writers[writer][dset_type]
                params['first_dset'] = offsets[writer]
                params['num_dsets'] = counts[writer]
                params['start'] = 0
                params['stride'] = 1 * dset_type_cfg['shots_per_sample']
        else:
            first_writers, writers_per_dset = util.divide_evenly(num_writers, num_dsets)
            for dset in range(num_dsets):
                first_writer = first_writers[dset]
                num_writers_this_dset = writers_per_dset[dset]
                for start, writer in enumerate(range(first_writer, first_writer + num_writers_this_dset)):
                    params = writers[writer][dset_type]
                    params['first_dset'] = dset
                    params['num_dsets'] = 1
                    params['start'] = start * dset_type_cfg['shots_per_sample']
                    params['stride'] = num_writers_this_dset * dset_type_cfg['shots_per_sample']
    return writers


def prepare_output_directory(config):
    rundir = os.path.join(config['rootdir'], config['rundir'])
    if os.path.exists(rundir):
        if config['force']:
            print("ana_daq_driver: Removing directory %s" % rundir)
            shutil.rmtree(rundir)
        else:
            print("FATAL: the directory %s exists, and --force was not given. "
                  "Use a different value for --rundir or pass --force to overwrite" % rundir)
            sys.exit(1)
            
    os.mkdir(rundir)
    hdf5_path = os.path.join(rundir, 'hdf5')
    log_path =  os.path.join(rundir, 'logs')
    pid_path =  os.path.join(rundir, 'pids')
    results_path =  os.path.join(rundir, 'results')

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
       
 
def construct_daq_writer_command_lines(daq_writers, config, args):
    rundir = os.path.join(config['rootdir'], config['rundir'])
    h5dir = os.path.join(rundir, 'hdf5')
    logdir = os.path.join(rundir, 'logs')
    piddir = os.path.join(rundir, 'pids')
    assert os.path.exists(h5dir)
    assert os.path.exists(logdir)
    assert os.path.exists(piddir)

    group = 'daq_writers'
    command_lines = []
    for idx, writer in enumerate(daq_writers):
        num_shots = int(round(float(config['num_samples'])))
        small_name_first = int(writer['small']['first_dset'])
        vlen_name_first = int(writer['vlen']['first_dset'])
        detector_name_first = int(writer['detector']['first_dset'])
        small_name_count = int(writer['small']['num_dsets'])
        vlen_name_count = int(writer['vlen']['num_dsets'])
        detector_name_count = int(writer['detector']['num_dsets'])
        small_shot_first = int(writer['small']['start'])
        vlen_shot_first = int(writer['vlen']['start'])
        detector_shot_first = int(writer['detector']['start'])
        small_shot_stride = int(writer['small']['stride'])
        vlen_shot_stride = int(writer['vlen']['stride'])
        detector_shot_stride = int(writer['detector']['stride'])
        small_chunksize = int(config['daq_writers']['datasets']['small']['chunksize'])
        vlen_chunksize = int(config['daq_writers']['datasets']['vlen']['chunksize'])
        detector_chunksize = int(config['daq_writers']['datasets']['detector']['chunksize'])
        vlen_min_per_shot = int(config['daq_writers']['datasets']['vlen']['min_per_shot'])
        vlen_max_per_shot = int(config['daq_writers']['datasets']['vlen']['max_per_shot'])
        detector_rows = int(config['daq_writers']['datasets']['detector']['dim'][0])
        detector_columns = int(config['daq_writers']['datasets']['detector']['dim'][1])
        flush_interval = int(config['flush_interval'])
        writers_hang = int(config['writers_hang'])

        cmdlog_basename = '%s-s%4.4d.cmd.log' % (group, idx)
        cmdlog = os.path.join(logdir, cmdlog_basename)
        program = os.path.abspath('bin/daq_writer')
        path = os.environ['PATH']
        cmd = 'LD_LIBRARY_PATH="" PYTHONPATH="" PATH=%s %s %d %s %s %d ' % \
              (path,
               program,
               int(config['verbose']),
               rundir,
               group,
               idx)
        cmd += ' '.join(map(str,[num_shots,
                                 small_name_first,
                                 vlen_name_first,
                                 detector_name_first,
                                 small_name_count,
                                 vlen_name_count,
                                 detector_name_count,
                                 small_shot_first,
                                 vlen_shot_first,
                                 detector_shot_first,
                                 small_shot_stride,
                                 vlen_shot_stride,
                                 detector_shot_stride,
                                 small_chunksize,
                                 vlen_chunksize,
                                 detector_chunksize,
                                 vlen_min_per_shot,
                                 vlen_max_per_shot,
                                 detector_rows,
                                 detector_columns,
                                 flush_interval,
                                 writers_hang]))
        if not args.writer:
            cmd += ' >%s 2>&1' % cmdlog
        command_lines.append(cmd)
    return command_lines


def check_structure(config):
    structure = {'force':'value',
                 'rootdir':'value',
                 'rundir':'value',
                 'num_samples':'value',
                 'verbose':'value',
                 'flush_interval':'value',
                 'time':'value',
                 'writers_hang':'value',
                 'lfs':{'do_stripe':'value',
                        'stripe_size_mb':'value',
                        'OST_start_index':'value',
                        'count':'value',
                    },
                 'daq_writers':{'num':'value',
                                'num_per_host':'value',
                                'hosts':'list',
                                'datasets':{'small':{'num':'value',
                                                     'chunksize':'value',
                                                     'shots_per_sample':'value',
                                                     },
                                            'vlen':{'num':'value',
                                                    'chunksize':'value',
                                                    'shots_per_sample':'value',
                                                    'min_per_shot':'value',
                                                    'max_per_shot':'value',
                                                    
                                                     },
                                            'detector':{'num':'value',
                                                        'chunksize':'value',
                                                        'shots_per_sample':'value',
                                                        'dim':'list'
                                                     },
                                        },
                            },
                 'daq_master':{'num':'value',
                               'num_per_host':'value',
                               'hosts':'list',
                               },
                 'ana_reader_master':{'num':'value',
                                'num_per_host':'value',
                                'hosts':'list',
                               
                                  },
                 'ana_reader_stream':{'num':'value',
                                'num_per_host':'value',
                                'hosts':'list',
                               },
             }
    def recurse_check(correct, config, stack):
        assert isinstance(correct, dict), "check is not a dict at %s" % stack
        assert isinstance(config, dict), "config is not a dict at %s" % stack
        assert len(correct)==len(config), "%s should have %d items, but it has %d" % \
            (stack, len(correct), len(config))
        for ky, value in correct.items():
            assert ky in config, "config at %s is missing %s" % (stack, ky)
            if isinstance(value, dict):
                recurse_check(value, config[ky], [xx for xx in stack] + [ky])
            else:
                assert value in ['value','list']
                if value == 'value':
                    isval = isinstance(config[ky],str) or isinstance(config[ky],int) or \
                            isinstance(config[ky],float) or isinstance(config[ky],bool)
                    assert isval, "%s is not a value, it is %s" % (stack, config[ky])
                elif value == 'list':
                    assert isinstance(config[ky],list)

    recurse_check(structure, config, ['root'])


def check_config(config):
    check_structure(config)
    assert os.path.exists(config['rootdir'])
    for group in ['daq_writers','ana_reader_master', 'ana_reader_stream']:
        cfg = config[group]
        if cfg['num']<=0:
            continue
        hosts = cfg['hosts']
        assert isinstance(hosts, list)
        num_hosts = len(hosts)
        hosts_needed = int(math.ceil(cfg['num']/cfg['num_per_host']))
        assert hosts_needed <= num_hosts, ("%s: not enought hosts (%d) to support " + \
                                           "%d processes at %d per host") % (group, num_hosts, cfg['num'], cfg['num_per_host'])
    vlen_min = config['daq_writers']['datasets']['vlen']['min_per_shot']
    vlen_max = config['daq_writers']['datasets']['vlen']['max_per_shot']
    assert vlen_min <= vlen_max, "vlen min > max"
    detdim = config['daq_writers']['datasets']['detector']['dim']
    assert isinstance(detdim, list)
    assert 2 == len(detdim), "detector dim must have 2 values"


def assign_hosts(group, config):
    cfg = config[group]
    if cfg['num'] == 0:
        return []
    all_hosts = copy.deepcopy(config[group]['hosts'])
    hosts = [all_hosts[idx//cfg['num_per_host']] for idx in range(cfg['num'])]
    return hosts

  
def run(argv):
    parser = getParser()
    args = parser.parse_args(argv[1:])
    if not args.config:
        assert os.path.exists('config.yaml'), "use --config to specify config file"
        print("using config.yaml for configuration")
        args.config = 'config.yaml'
    
    config = yaml.load(open(args.config,'r'))
    check_config(config)
    for ky in ['force','rootdir','rundir','verbose','flush_interval','time','writers_hang']:
        val = getattr(args,ky)
        if val in [None, False]: continue
        print("replacing config[%s] with %s (from command line)" % (ky,val))
        config[ky]=val

    jobs = Jobs(config)

    if args.kill:
        jobs.kill_all()
        return

    prepare_output_directory(config)
    daq_writers = divide_datasets_between_writers(config)
    daq_writer_commands = construct_daq_writer_command_lines(daq_writers, config, args)

    if args.writer:
        print("=======")
        print(" running daq_writer")
        cmd = daq_writer_commands[0]
        print(cmd)
        os.system(cmd)
        return
        
    daq_writer_hosts = assign_hosts('daq_writers', config)

    jobs.launch('daq_writers', daq_writer_commands, daq_writer_hosts)
    time.sleep(1)
    jobs.wait()
    
    pprint(config)
    pprint(daq_writers)
    pprint(daq_writer_commands)
    pprint(daq_writer_hosts)
    
if __name__ == '__main__':
    run(sys.argv)
    
