#!/bin/bash

export SROOTBASE="/cvmfs/icecube.opensciencegrid.org/py2-v2" ;
export SROOT="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64" ;
export I3_PORTS="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports" ;
export I3_DATA="/cvmfs/icecube.opensciencegrid.org/py2-v2/../data" ;
export I3_TESTDATA="/cvmfs/icecube.opensciencegrid.org/py2-v2/../data/i3-test-data" ;
export PATH="/usr/lib64/openmpi/bin:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/root-v5.34.18/bin:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/bin:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/bin:/usr/lib64/qt-3.3/bin:/opt/pgi/linux86-64/13.3/bin:/usr/local/bin:/bin:/usr/bin:/opt/puppetlabs/bin:/opt/dell/srvadmin/bin" ;
export MANPATH="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/man:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/share/man::/opt/pgi/linux86-64/13.3/man" ;
export PKG_CONFIG_PATH="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib/pkgconfig:" ;
export LD_LIBRARY_PATH="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/root-v5.34.18/lib:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/lib::/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/lib:/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/jre/lib:/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/jre/lib/amd64:/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/jre/lib/amd64/server:/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/jre/lib/i386:/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64/jre/lib/i386/server" ;
export PYTHONPATH="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/root-v5.34.18/lib:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib/python2.7/site-packages:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/lib/python2.7/site-packages:" ;
export ROOTSYS="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/i3ports/root-v5.34.18" ;
export OS_ARCH="RHEL_6_x86_64" ;
export GCC_VERSION="4.4.7" ;
export JAVA_HOME="/cvmfs/icecube.opensciencegrid.org/py2-v2/../distrib/jdk1.6.0_24_RHEL_6_x86_64" ;
export GOTO_NUM_THREADS="1" ;
export PERL5LIB="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib/perl:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib/perl5:/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/lib/perl5/site_perl:" ;
export GLOBUS_LOCATION="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64" ;
export X509_CERT_DIR="/cvmfs/icecube.opensciencegrid.org/py2-v2/RHEL_6_x86_64/share/certificates" ;
export OPENCL_VENDOR_PATH="/etc/OpenCL/vendors" ;

export PYTHONPATH=/home/dschultz/.local/lib/python2.7/site-packages:$PYTHONPATH
export LD_LIBRARY_PATH=/home/dschultz/.local/lib/python2.7/site-packages:$LD_LIBRARY_PATH

export CONDOR_CONFIG=/home/dschultz/condor_config/condor_config
python /home/dschultz/github/scripts/condor_history_to_elasticsearch.py -a http://mongo-ssd-test.icecube.wisc.edu:9200 --collectors glidein-simprod.icecube.wisc.edu cm.icecube.wisc.edu
