#!/usr/bin/env python
"""
Parallel clusterized gridftp sync
"""

import os
import time
import subprocess
from functools import partial
from multiprocessing.pool import ThreadPool

source = '/mnt/lfs4/simprod/dagtemp2/20009'
dest = 'gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/20009'

pool = ThreadPool(100)

for root,dirs,files in os.walk(source):
    for f in files:
        s = os.path.join(root,f)
        d = s.replace(source,dest)
        cmd = 'globus-url-copy -sync -v -cd -rst file://%s %s'%(s,d)
        pool.apply(partial(subprocess.call,cmd,shell=True))
pool.join()
