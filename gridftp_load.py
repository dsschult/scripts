#!/bin/sh /cvmfs/icecube.opensciencegrid.org/py2-v2/icetray-start
#METAPROJECT combo/stable

import os
import math
import random
import subprocess
import tempfile
import string
import logging
import hashlib

import tornado.ioloop
import tornado.gen

def cksm(filename,buffersize=16384,file=True):
    """Return checksum of file using algorithm specified"""
    digest = hashlib.sha512()
    if filename and os.path.exists(filename):
        # checksum file contents
        with open(filename) as filed:
            buffer = filed.read(buffersize)
            while buffer:
                digest.update(buffer)
                buffer = filed.read(buffersize)
    else:
        # just checksum the contents of the first argument
        digest.update(filename)
    return digest.hexdigest()

class Cache:
    def __init__(self, server):
        self.server = server
        self.dir = tempfile.mkdtemp(dir=os.getcwd())

    def create(self, name, size):
        logging.info('create %s %d',name,size)
        size = int(random.gauss(size,size*.05) * 1024**2)
        with open(os.path.join(self.dir,name),'wb') as f:
            for _ in range(size//1024):
                f.write(os.urandom(1024))
            if size%1024 > 0:
                f.write(os.urandom(size%1024))

    def download(self, name):
        logging.info('download %s',name)
        subprocess.check_call(['globus-url-copy','-rst',os.path.join(self.server,name),
                               'file:'+os.path.join(self.dir,name)])

    def upload(self, name):
        logging.info('upload %s',name)
        fname = os.path.join(self.dir,name)
        c = cksm(fname)
        
        subprocess.check_call(['globus-url-copy','-rst','-cd',
                               'file:'+fname,
                               os.path.join(self.server,name)])
        subprocess.check_call(['globus-url-copy','-rst',
                               os.path.join(self.server,name),
                               'file:'+fname+'_tmp'])
        try:
            if c != cksm(fname+'_tmp'):
                raise Exception('upload error')
        finally:
            os.remove(fname+'_tmp')

    def remove_local(self, name):
        logging.info('remove_local %s',name)
        try:
            os.remove(os.path.join(self.server,name))
        except:
            pass

    def remove(self, name):
        logging.info('remove %s',name)
        #subprocess.call(['uberftp','-rm','-r',os.path.join(self.server,name)])
        try:
            os.remove(os.path.join(self.server,name))
        except:
            pass

def wait(t):
    logging.info('wait %.2f',t)
    t = random.lognormvariate(math.log(t*1.1), 0.3)
    return tornado.gen.sleep(t*3600)

class Corsika:
    def __init__(self, cache):
        self.cache = cache
        self.name = ''.join(random.sample(string.ascii_letters,24))
        self.dag = [(self.generate,self.generate_bg),self.hits,self.detector,self.filtering]
    @tornado.gen.coroutine
    def generate(self):
        yield wait(1.12)
        self.cache.create(self.name, 315)
        self.cache.upload(self.name)
        self.cache.remove_local(self.name)
    @tornado.gen.coroutine
    def generate_bg(self):
        yield wait(0.17)
        self.cache.create(self.name+'_bg', 45)
        self.cache.upload(self.name+'_bg')
        self.cache.remove_local(self.name+'_bg')
    @tornado.gen.coroutine
    def hits(self):
        self.cache.download(self.name)
        self.cache.download(self.name+'_bg')
        self.cache.remove_local(self.name)
        self.cache.remove_local(self.name+'_bg')
        yield wait(3.48)
        self.cache.create(self.name+'_hits', 220)
        self.cache.upload(self.name+'_hits')
        self.cache.remove_local(self.name+'_hits')
    @tornado.gen.coroutine
    def detector(self):
        self.cache.download(self.name+'_hits')
        self.cache.remove_local(self.name+'_hits')
        yield wait(0.5)
        self.cache.create(self.name+'_det', 185)
        self.cache.upload(self.name+'_det')
        self.cache.remove_local(self.name+'_det')
    @tornado.gen.coroutine
    def filtering(self):
        self.cache.download(self.name+'_det')
        self.cache.remove_local(self.name+'_det')
        yield wait(0.9)
        self.cache.remove(self.name)
        self.cache.remove(self.name+'_bg')
        self.cache.remove(self.name+'_hits')
        self.cache.remove(self.name+'_det')

class NuGen_Systematics:
    def __init__(self, cache):
        self.cache = cache
        self.name = ''.join(random.sample(string.ascii_letters,24))
        self.dag = [(self.generate,self.generate_bg),self.hits,
                    [self.detector for _ in range(4)],
                    [self.filtering for _ in range(4)]]
    @tornado.gen.coroutine
    def generate(self):
        yield wait(0.015)
        self.cache.create(self.name, 13)
        self.cache.upload(self.name)
        self.cache.remove_local(self.name)
    @tornado.gen.coroutine
    def generate_bg(self):
        yield wait(0.02)
        self.cache.create(self.name+'_bg', 9.5)
        self.cache.upload(self.name+'_bg')
        self.cache.remove_local(self.name+'_bg')
    @tornado.gen.coroutine
    def hits(self):
        self.cache.download(self.name)
        self.cache.download(self.name+'_bg')
        self.cache.remove_local(self.name)
        self.cache.remove_local(self.name+'_bg')
        yield wait(0.67)
        for i in range(4):
            n = self.name+'_hits_%d'%i
            self.cache.create(n, 6.4)
            self.cache.upload(n)
            self.cache.remove_local(n)
    @tornado.gen.coroutine
    def detector(self):
        for i in range(4):
            n = self.name+'_hits_%d'%i
            self.cache.download(n)
            self.cache.remove_local(n)
        yield wait(0.23)
        for i in range(4):
            n = self.name+'_det_%d'%i
            self.cache.create(n, 4.7)
            self.cache.upload(n)
            self.cache.remove_local(n)
    @tornado.gen.coroutine
    def filtering(self):
        for i in range(4):
            n = self.name+'_det_%d'%i
            self.cache.download(n)
            self.cache.remove_local(n)
        yield wait(0.14)
        self.cache.remove(self.name)
        self.cache.remove(self.name+'_bg')
        for i in range(4):
            self.cache.remove(self.name+'_hits_%d'%i)
            self.cache.remove(self.name+'_det_%d'%i)


@tornado.gen.coroutine
def run_dag(job):
    for level in job.dag:
        tasks = []
        if isinstance(level,(tuple,list)):
            tasks = list(level)
        else:
            tasks = [level]
        while True:
            try:
                yield [i() for i in tasks]
            except:
                logging.warn('error running task', exc_info=True)
            else:
                break


def run(args):
    cache = Cache('gsiftp://gridftp-scratch.icecube.wisc.edu/local/simprod/')
    #cache = Cache('gsiftp://gridftp-3.icecube.wisc.edu/mnt/lfs4/simprod/dagtemp2/test/')
    ioloop = tornado.ioloop.IOLoop.current()
    for i in range(args.num):
        ioloop.call_later(i*60,run_dag,Corsika(cache))
        for t in range(1,101):
            ioloop.call_later(i*t*6,run_dag,NuGen_Systematics(cache))
    logging.warn('starting')
    ioloop.start()
    logging.warn('done')

def submit(args):
    p = subprocess.Popen(['condor_submit','-'], stdin=subprocess.PIPE,
                         cwd=os.getcwd())
    p.communicate("""
executable = gridftp_load.py
#getenv = True
output = out.$(PROCESS)
error = err.$(PROCESS)
log = log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = /tmp/x509up_u21458
env = X509_USER_PROXY=x509up_u21458
x509userproxy = /tmp/x509up_u21458
request_disk = 30000000
request_machine_token = 1
#Requirements = (HAS_CVMFS_icecube_opensciencegrid_org =?= true || HAS_CVMFS_oasis_opensciencegrid_org =?= true || ICECUBE_CVMFS_Exists =?= true)
arguments = --num 50 --log_level error
queue %d"""%(args.num//50))
    p.wait()

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Simulate load on gridftp server')
    parser.add_argument('-n','--num', type=int, default=100, help='number of jobs')
    parser.add_argument('--condor_submit', default=False, action='store_true',
                        help='run condor_submit to parallelize')
    parser.add_argument('--log_level', type=str, default='info',
                        choices={'debug','info','warn','error'},
                        help='log level (default: info)')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging,args.log_level.upper()))

    if args.condor_submit:
        submit(args)
    else:
        run(args)

if __name__ == '__main__':
    main()
