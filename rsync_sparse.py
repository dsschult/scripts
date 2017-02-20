#!/usr/bin/env python
"""
``rsync`` only file attributes, not actual data.
"""
from __future__ import print_function

import sys
import os
import subprocess
import time
from datetime import datetime

def get_mod(s):
    mod = [(4 if s[0] == 'r' else 0) | (2 if s[1] == 'w' else 0) | (1 if s[2] == 'x' else 0),
           (4 if s[3] == 'r' else 0) | (2 if s[4] == 'w' else 0) | (1 if s[5] == 'x' else 0),
           (4 if s[6] == 'r' else 0) | (2 if s[7] == 'w' else 0) | (1 if s[8] == 'x' else 0),
          ]
    return mod[0]<<6 | mod[1]<<3 | mod[2]

def get_timestring(t):
    d = datetime.strptime(t, '%Y/%m/%d-%H:%M:%S')
    return d.strftime('%Y%m%d%H%M.%S')

def process(line,dirs=False):
    curdir = os.getcwd()
    for chr in '\n "\'':
        line = line.strip(chr)
    try:
        parts = [x.strip() for x in line.split() if x.strip()]
        print(parts)
        chmod = get_mod(parts[1])
        mod_time = get_timestring(parts[3])
        path = parts[4]
        if parts[0].startswith('>f'):
            # a regular file
            size = int(parts[2])
            with open(path,'w') as f:
                f.truncate(size)
            os.chmod(path, chmod)
            subprocess.call(['touch','-h','-t',mod_time,path])
        elif parts[0].startswith('cd'):
            # a directory
            path = path.strip('/')
            if not os.path.exists(path):
                os.mkdir(path)
            if not dirs: # directories need to be processed later
                return line
            os.chmod(path, chmod)
            subprocess.call(['touch','-h','-t',mod_time,path])
        elif parts[0].startswith('cL'):
            # a link
            linkpath = parts[-1]
            if linkpath.startswith('/'):
                if not os.path.islink(path):
                    os.symlink(linkpath[1:], path)
                subprocess.call(['touch','-h','-t',mod_time,path])
            else:
                os.chdir(os.path.dirname(path))
                if not os.path.islink(path):
                    os.symlink(parts[-1], os.path.basename(path))
                subprocess.call(['touch','-h','-t',mod_time,os.path.basename(path)])
        elif parts[0].startswith('.'):
            return # skip
        else:
            raise Exception('unknown file type')
    finally:
        os.chdir(curdir)

def read(input, output):
    cmd = ['rsync','-a','-n','--out-format="%i %B %l %M %n%L"',input,output]
    print(' '.join(cmd))
    postprocess = []
    with open(os.devnull, 'w') as fnull:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=fnull)
        while True:
            line = p.stdout.readline()
            print(line,)
            if not line:
                break
            ret = process(line)
            if ret:
                postprocess.append(ret)

        if p.poll() is None:
            p.terminate()
    for line in postprocess:
        process(line,dirs=True)
    print('done')

if __name__ == '__main__':
    read(sys.argv[1],sys.argv[2])
