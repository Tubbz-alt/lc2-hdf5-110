#!/usr/bin/env python

import os
import sys
import copy
import math
import time
import shutil
import yaml
import platform
import getpass
import paramiko as pm
import argparse
from pprint import pprint

import ana_daq_util as util


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
    parser.add_argument('-p', '--prefix', type=str, 
                        help="name of subdirectory from root dir to run from", default=None)
    parser.add_argument('-c', '--config', type=str, 
                        help="config file", default=None)
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


def prepare_output_directory(config, args):
    basedir = config['output']['root']
    expdir = os.path.join(basedir, args.prefix)
    if os.path.exists(expdir):
        if args.force:
            print("ana_daq_driver: Removing directory %s" % expdir)
            shutil.rmtree(expdir)
        else:
            print("FATAL: the directory %s exists, and --force was not given. "
                  "Use a different --prefix or pass --force to overwrite" % expdir)
            sys.exit(1)
            
    os.mkdir(expdir)
    hdf5_path = os.path.join(expdir, 'hdf5')
    log_path =  os.path.join(expdir, 'logs')
    results_path =  os.path.join(expdir, 'results')

    os.mkdir(hdf5_path)
    os.mkdir(log_path)
    os.mkdir(results_path)
    
    if config['output']['lfs']['do_stripe']:
        stripe_size_mb = config['output']['lfs']['stripe_size_mb']
        OST_start_index = config['output']['lfs']['OST_start_index']
        count = config['output']['lfs']['count']
        assert isinstance(stripe_size_mb, int)
        assert stripe_size_mb>=1 and stripe_size_mb < 1024
        stripe_size = stripe_size_mb << 20
        cmd = 'lfs setstripe --stripe-size %d --index %d --count %d %s' % \
              (stripe_size, OST_start_index, count, hdf5_path)
        print("striping hdf5 subdir:\n%s" % cmd)
        assert 0 == os.system(cmd)
       
 
def construct_daq_writer_command_lines(daq_writers, config, args):
    command_lines = []
    h5dir = os.path.join(config['output']['root'], args.prefix, 'hdf5')
    logdir = os.path.join(config['output']['root'], args.prefix, 'logs')
    assert os.path.exists(h5dir)
    assert os.path.exists(logdir)
    for idx, writer in enumerate(daq_writers):
        fname = '%s-s%4.4d' % (args.prefix, idx) 
        h5output = os.path.join(h5dir, fname + '.h5')
        logstd = os.path.join(logdir, fname + '.stdout.log')
        logerr = os.path.join(logdir, fname + '.stderr.log')
        num_shots = int(round(float(config['num_samples'])))
        small_name_first = writer['small']['first_dset']
        vlen_name_first = writer['vlen']['first_dset']
        detector_name_first = writer['detector']['first_dset']
        small_name_count = writer['small']['num_dsets']
        vlen_name_count = writer['vlen']['num_dsets']
        detector_name_count = writer['detector']['num_dsets']
        small_shot_first = writer['small']['start']
        vlen_shot_first =  writer['vlen']['start']
        detector_shot_first =  writer['detector']['start']
        small_shot_stride = writer['small']['stride']
        vlen_shot_stride = writer['vlen']['stride']
        detector_shot_stride = writer['detector']['stride']
        small_chunksize = config['daq_writers']['datasets']['small']['chunksize']
        vlen_chunksize = config['daq_writers']['datasets']['vlen']['chunksize']
        detector_chunksize = config['daq_writers']['datasets']['detector']['chunksize']
        vlen_min_per_shot = config['daq_writers']['datasets']['vlen']['min_per_shot']
        vlen_max_per_shot = config['daq_writers']['datasets']['vlen']['max_per_shot']
        detector_rows = config['daq_writers']['datasets']['detector']['dim'][0]
        detector_columns = config['daq_writers']['datasets']['detector']['dim'][1]
        flush_interval = config['flush_interval']
        
        cmd = 'bin/daq_writer %d %s %s %s ' % \
              (config['verbose'],
               h5output,
               logstd,
               logerr)
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
                                 flush_interval]))
        command_lines.append(cmd)
    return command_lines


def check_config(config):
    assert os.path.exists(config['output']['root'])
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


def assign_hosts(group, config):
    cfg = config[group]
    if cfg['num'] == 0:
        return []
    all_hosts = copy.deepcopy(config[group]['hosts'])
    hosts = [all_hosts[idx//cfg['num_per_host']] for idx in range(cfg['num'])]
    return hosts


class Jobs(object):
    def __init__(self, config):
        self.config = config
        self.host2ssh = {}
        for group in ['daq_writers', 'ana_reader_master', 'ana_reader_stream']:
            for host in config[group]['hosts']:
                if host not in self.host2ssh:
                    self.host2ssh[host]=None
        self.username = getpass.getuser()
        assert self.username, "no username found"
        self._launched = []
        
    def _makeSSH(self, host):
        if host == 'local':
            host = platform.node()
            print("local host is %s" % host)
        ssh = pm.SSHClient()
        ssh.set_missing_host_key_policy(pm.AutoAddPolicy())
        print("using paramiko to make ssh connection to host=%s as username=%s" % (host, self.username))
        ssh.connect(host, self.username)
        return ssh
    
    def launch(self, group, commands, hosts):
        assert len(commands)==len(hosts)
        idx = -1
        for command, host in zip(commands, hosts):
            idx += 1
            if self.host2ssh[host] is None:
                self.host2ssh[host] = self._makeSSH(host)
            ssh = self.host2ssh[host]
            print("launching: group=%s idx=%d" % (group,idx))
            stdin, stdout, stderr = ssh.exec_command(command)
            execdict = {'stdin':stdin, 'stdout':stdout, 'stderr':stderr,
                        'command':command, 'host':host, 'group':group, 'idx':idx}
            self._launched(execdict)

    def wait(self):
        for execdict in self._launched:
            res = execdict['stdout'].channel.recv_exit_status()
            group = execdict['group']
            idx = execdict['idx']
            stdout = '\n  '.join(execdict['stdout'].readlines())
            stderr = '\n  '.join(execdict['stderr'].readlines())
            host = execdict['host']
            command = execdict['command']
            if res != 0:
                msg = "*FAIL:    "
            else:
                msg = "*SUCCESS: "
            msg += "group=%s idx=%d host=%s res=%d command:\n"
            msg += "  %s\n" % command
            msg += "stdout:\n  %s" % stdout
            msg += "stderr:\n  %s" % stderr
            print(msg)
    

def run(argv):
    parser = getParser()
    args = parser.parse_args(argv[1:])
    if not args.config:
        assert os.path.exists('config.yaml'), "use --config to specify config file"
        print("using config.yaml for configuration")
        args.config = 'config.yaml'
    assert args.prefix, "use --prefix to specify prefix, subdir to root for output"
    
    config = yaml.load(open(args.config,'r'))
    check_config(config)
    prepare_output_directory(config, args)
    daq_writers = divide_datasets_between_writers(config)
    daq_writer_commands = construct_daq_writer_command_lines(daq_writers, config, args)
    daq_writer_hosts = assign_hosts('daq_writers', config)

    jobs = Jobs(config)
    jobs.launch('daq_writers', daq_writer_commands, daq_writer_hosts)
    time.sleep(1)
    jobs.wait()
    
    pprint(config)
    pprint(daq_writers)
    pprint(daq_writer_commands)
    pprint(daq_writer_hosts)
    
if __name__ == '__main__':
    run(sys.argv)
    
