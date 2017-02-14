#!/usr/bin/env python
"""
Parallel clusterized gridftp sync
"""

import os
import time
import subprocess

source = '/mnt/lfs4/simprod/dagtemp2/20009'
dest = 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/20009'

class CondorPool():
    # make a 'threadpool' using HTCondor
    def __init__(self, num):
        self.num = num
        self.queue = []
        self.processes = []
    def _submit(self, cmd):
        print(cmd)
        submission = ['condor_run',
                      '-a','request_machine_token=1',
                      '-a','should_transfer_files=YES',
                      '-a','transfer_input_files=/tmp/x509up_u21458',
                      '-a','env=X509_USER_PROXY=x509up_u21458',
                      '-a','x509userproxy=/tmp/x509up_u21458',
                      cmd]
        self.processes.append(subprocess.Popen(submission))
    def apply(self, cmd):
        if len(self.processes) >= self.num:
            self.queue.append(cmd)
        else:
            self._submit(cmd)
    def join(self):
        while self.queue or self.processes:
            still_running = []
            for p in self.processes:
                if p.poll() is None:
                    still_running.append(p)
            self.processes = still_running
            while self.queue and len(self.processes) < self.num:
                self._submit(self.queue.pop())
            time.sleep(1)

pool = CondorPool(100)

for root,dirs,files in os.walk(source):
    for f in files:
        s = os.path.join(root,f)
        d = s.replace(source,dest)
        cmd = 'globus-url-copy -sync -cd -rst file://%s %s'%(s,d)
        pool.apply(cmd)
pool.join()
