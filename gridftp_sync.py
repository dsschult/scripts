#!/usr/bin/env python

import os
import subprocess
from multiprocessing.pool import ThreadPool

source = '/mnt/lfs4/simprod/dagtemp2/20008'
dest = 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/20008'

pool = ThreadPool(16)

def sync(s,d):
    subprocess.check_call('globus-url-copy -cd -rst file:%s %s'%(s,d),
                          shell=True)

for root,dirs,files in os.walk(source):
    for f in files:
        s = os.path.join(root,f)
        d = s.replace(source,dest)
        print s,d
        raise Exception()
        pool.apply_async(sync,(s,d))
