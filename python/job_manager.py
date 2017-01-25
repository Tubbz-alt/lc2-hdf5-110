import os
import glob
import getpass
import time
import platform
from pprint import pprint

def parse_pid_ln(orig_ln):
    ln, pid = orig_ln.split('pid=')
    pid = int(pid)
    ln, hostname = ln.split('hostname=')
    hostname = hostname.strip()
    ln, idx = ln.split('idx=')
    idx = int(idx)
    ln, group = ln.split('group=')
    group = group.strip()
    return group, idx, hostname, pid

class Jobs(object):
    def __init__(self, config):
        self.config = config
        self.username = getpass.getuser()
        assert self.username, "no username found"
        self._launched = []
        
    def launch(self, group, commands, hosts):
        assert len(commands) == len(hosts)
        idx = -1
        for command, host in zip(commands, hosts):
            idx += 1
            if host == 'local':
                ssh_command = "%s&" % command
            else:
                ssh_command = "ssh %s %s&" % (host, command)
            print("launching: group=%s idx=%d -- %s" % (group, idx, ssh_command))
            res = os.system(ssh_command)
            assert res==0, "problem launching group=%s idx=%d cmd=%s\nres=%d" % \
                (group, idx, ssh_command, res)
            self._launched.append({'group':group, 'idx':idx, 'cmd':ssh_command, 'host':host})

    def wait(self):
        def remove_if_done(waiting_for):
            done = set()
            for fname in waiting_for:
                if os.path.exists(fname):
                    print("  wait - done: %s" % os.path.basename(fname))
                    done.add(fname)
            return waiting_for.difference(done)

        t0 = time.time()
        logdir = os.path.join(self.config['rootdir'], self.config['rundir'], 'logs')
        assert os.path.exists(logdir)
        waiting_for = set()
        for launched in self._launched:
            basename = launched['group'] + '-s' + ('%4.4d' % launched['idx'])
            finished_fname = os.path.join(logdir, basename + '.finished')
            if finished_fname in waiting_for:
                print("WARNING: wait - finished file already in list: %s" % finished_fname)
            waiting_for.add(finished_fname)
        if len(waiting_for)==0:
            print("job_manager.wait - no jobs to wait for")
            return
        print("job_manager.wait - waiting for %d jobs" % len(waiting_for))
        pprint(waiting_for)
        while True:
            waiting_for = remove_if_done(waiting_for)
            if len(waiting_for)==0:
                print("wait - all jobs done")
                break
            if self.config['time']>0:
                time_waited = time.time()-t0
                if time_waited > self.config['time']:
                    print("wait - hit time delay - killing all remaining jobs")
                    self.kill_all(waiting_for)
                    break
            time.sleep(3)

    def kill_all(self, waiting_for=None):
        def filter_based_on(fnames, waiting_for):
            if None is waiting_for:
                return fnames

            waiting_for_basenames = [os.path.splitext(os.path.basename(fname))[0] \
                                     for fname in waiting_for]
            filtered = []
            for fname in fnames:
                basename = os.path.splitext(os.path.basename(fname))[0]
                if basename in waiting_for_basenames:
                    filtered.append(fname)
            return set(filtered)
                
        local_host = platform.node()
        pid_dir = os.path.join(self.config['rootdir'], self.config['rundir'], 'pids')
        assert os.path.exists(pid_dir), "pid_dir doesn't exist: %s" % pid_dir
        pid_files = glob.glob(os.path.join(pid_dir, "*.pid"))
        if len(pid_files)==0:
            print("no pid files found in %s" % pid_dir)
            return
        pid_files = filter_based_on(pid_files, waiting_for)
        print("%d pid files will be used, from dir %s" % (len(pid_files), pid_dir))
        for fname in pid_files:
            ln = open(fname,'r').read().strip()
            assert len(ln.split('\n'))==1
            group, idx, hostname, pid = parse_pid_ln(ln)
            kill_cmd = 'kill -9 %d' % pid
            if hostname != local_host:
                kill_cmd = 'ssh %s %s' % (hostname, kill_cmd)
            print("group=%s idx=%d hostname=%s pid=%d -- attempting kill with %s" % 
                  (group, idx, hostname, pid, kill_cmd))
            res = os.system(kill_cmd)
            if res == 0:
                print("   success")
            else:
                print("   *FAIL* res=%d" % res)
                              
