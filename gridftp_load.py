#!/usr/bin/env python

import os
import math
import random
import subprocess
import tempfile
import string
import logging

import tornado.ioloop
import tornado.gen

def cksm(filename,type='sha512',buffersize=16384,file=True):
    """Return checksum of file using algorithm specified"""
    if type not in ('md5','sha1','sha256','sha512'):
        raise Exception('cannot get checksum for type %r',type)

    try:
        digest = getattr(hashlib,type)()
    except:
        raise Exception('cannot get checksum for type %r',type)

    if file and os.path.exists(filename):
        # checksum file contents
        filed = open(filename)
        buffer = filed.read(buffersize)
        while buffer:
            digest.update(buffer)
            buffer = filed.read(buffersize)
        filed.close()
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
        for ending in ('.sha512sum','.sha256sum','.sha1sum','.md5sum'):
            subprocess.call(['globus-url-copy','-rst',os.path.join(self.server,name)+ending,
                             'file:'+os.path.join(self.dir,name)+ending])
        subprocess.check_call(['globus-url-copy','-rst',os.path.join(self.server,name),
                               'file:'+os.path.join(self.dir,name)])

    def upload(self, name):
        logging.info('upload %s',name)
        fname = os.path.join(self.dir,name)
        subprocess.check_call(['globus-url-copy','-rst','-cd',
                               'file:'+fname,
                               os.path.join(self.server,name)])
        subprocess.check_call(['globus-url-copy','-rst',
                               os.path.join(self.server,name),
                               'file:'+fname+'_tmp'])
        try:
            if cksm(fname) != cksm(fname+'_tmp'):
                raise Exception('checksum error')
        finally:
            os.remove(fname+'_tmp')

    def remove(self, name):
        logging.info('remove %s',name)
        subprocess.call(['uberftp','-rm','-r',os.path.join(self.server,name)])
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
    @tornado.gen.coroutine
    def generate_bg(self):
        yield wait(0.17)
        self.cache.create(self.name+'_bg', 45)
        self.cache.upload(self.name+'_bg')
    @tornado.gen.coroutine
    def hits(self):
        self.cache.download(self.name)
        self.cache.download(self.name+'_bg')
        yield wait(3.48)
        self.cache.create(self.name+'_hits', 220)
        self.cache.upload(self.name+'_hits')
    @tornado.gen.coroutine
    def detector(self):
        self.cache.download(self.name+'_hits')
        yield wait(0.5)
        self.cache.create(self.name+'_det', 185)
        self.cache.upload(self.name+'_det')
    @tornado.gen.coroutine
    def filtering(self):
        self.cache.download(self.name+'_det')
        yield wait(0.9)
        self.cache.remove(self.name)
        self.cache.remove(self.name+'_bg')
        self.cache.remove(self.name+'_hits')
        self.cache.remove(self.name+'_det')


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
    ioloop = tornado.ioloop.IOLoop.current()
    for _ in range(args.num):
        ioloop.add_callback(run_dag,Corsika(cache))
    logging.warn('starting')
    ioloop.start()
    logging.warn('done')

def submit(args):
    p = subprocess.Popen(['condor_submit','-'], stdin=subprocess.PIPE,
                         cwd=os.getcwd())
    p.communicate("""
executable = gridftp_load.py
getenv = True
output = out.$(PROCESS)
error = err.$(PROCESS)
log = log
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = /tmp/x509up_u21458
env = X509_USER_PROXY=x509up_u21458
x509userproxy = /tmp/x509up_u21458
request_disk = 200000000
request_machine_token = 1
arguments = --num 100 --log_level error
queue %d"""%(args.num//100))
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
