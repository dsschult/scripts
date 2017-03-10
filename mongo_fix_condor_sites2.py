from __future__ import print_function

from pymongo import MongoClient

db = MongoClient('localhost').condor

reserved_ips = {
    '18.12': 'MIT',
    '23.22': 'AWS',
    '35.9': 'AGLT2',
    '40.78': 'Azure',
    '40.112': 'Azure',
    '50.16': 'AWS',
    '50.17': 'AWS',
    '54.144': 'AWS',
    '54.145': 'AWS',
    '54.157': 'AWS',
    '54.158': 'AWS',
    '54.159': 'AWS',
    '54.161': 'AWS',
    '54.163': 'AWS',
    '54.166': 'AWS',
    '54.167': 'AWS',
    '54.197': 'AWS',
    '54.204': 'AWS',
    '54.205': 'AWS',
    '54.211': 'AWS',
    '54.227': 'AWS',
    '54.243': 'AWS',
    '72.36': 'Illinois',
    '128.9': 'osgconnect',
    '128.55': 'Berkeley',
    '128.84': 'NYSGRID_CORNELL_NYS1',
    '128.104': 'CHTC',
    '128.105': 'CHTC',
    '128.118': 'Bridges',
    '128.120': 'UCD',
    '128.205': 'osgconnect',
    '128.211': 'Purdue-Hadoop',
    '128.227': 'FLTech',
    '128.230': 'Syracuse',
    '129.74': 'NWICG_NDCMS',
    '129.93': 'Nebraska',
    '129.105': 'NUMEP-OSG',
    '129.107': 'UTA_SWT2',
    '129.119': 'SU-OG',
    '129.130': 'Kansas',
    '129.217': 'LIDO_Dortmund',
    '130.74': 'Miss',
    '130.127': 'Clemson-Palmetto',
    '130.199': 'BNL-ATLAS',
    '131.94': 'FLTECH',
    '131.215': 'CIT_CMS_T2',
    '131.225': 'USCMS-FNAL-WC1',
    '132.206': 'CA-MCGILL-CLUMEQ-T2',
    '133.82': 'Japan',
    '134.93': 'mainz',
    '136.145': 'osgconnect',
    '137.99': 'UConn-OSG',
    '137.135': 'Azure',
    '138.23': 'UCRiverside',
    '138.91': 'Azure',
    '141.34': 'DESY-HH',
    '142.150': 'CA-SCINET-T2',
    '142.244': 'Alberta',
    '144.92': 'HEP_WISC',
    '149.165': 'Indiana',
    '155.101': 'Utah',
    '163.118': 'FLTECH',
    '169.228': 'UCSDT2',
    '171.67': 'HOSTED_STANFORD',
    '174.129': 'AWS',
    '184.73': 'AWS',
    '192.5': 'Boston',
    '192.12': 'Colorado',
    '192.41': 'AGLT2',
    '192.84': 'Ultralight',
    '192.168': None,
    '192.170': 'MWT2',
    '193.58': 'T2B_BE_IIHE',
    '193.190': 'T2B_BE_IIHE',
    '198.32': 'osgconnect',
    '198.48': 'Hyak',
    '200.136': 'SPRACE',
    '200.145': 'SPRACE',
    '206.12': 'CA-MCGILL-CLUMEQ-T2',
    '216.47': 'MWT2',
}
reserved_ips.update({'10.%d'%i:None for i in range(256)})
reserved_ips.update({'172.%d'%i:None for i in range(16,32)})

reserved_domains = {
    'aglt2.org': 'AGLT2',
    'bridges.psc.edu': 'Bridges',
    'campuscluster.illinois.edu': 'Illinois',
    'cl.iit.edu': 'MWT2',
    'cm.cluster': 'LIDO_Dortmund',
    'cmsaf.mit.edu': 'MIT',
    'colorado.edu': 'Colorado',
    'cpp.ualberta.ca': 'Alberta',
    'crc.nd.edu': 'NWICG_NDCMS',
    'cci.wisc.edu': 'CHTC',
    'chtc.wisc.edu': 'CHTC',
    'cs.wisc.edu': 'CS_WISC',
    'cse.buffalo.edu': 'osgconnect',
    'discovery.wisc.edu': 'CHTC',
    'ec2.internal': 'AWS',
    'ember.arches': 'Utah',
    'fnal.gov': 'USCMS-FNAL-WC1',
    'grid.tu-dortmund.de': 'LIDO_Dortmund',
    'guillimin.clumeq.ca': 'Guillimin',
    'hcc.unl.edu': 'Crane',
    'hep.caltech.edu': 'CIT_CMS_T2',
    'hep.int': 'osgconnect',
    'hep.olemiss.edu': 'Miss',
    'hep.wisc.edu': 'HEP_WISC',
    'icecube.wisc.edu': 'NPX',
    'ics.psu.edu': 'Bridges',
    'iihe.ac.be': 'T2B_BE_IIHE',
    'internal.cloudapp.net': 'osgconnect',
    'isi.edu': 'osgconnect',
    'iu.edu': 'Indiana',
    'lidocluster.hp': 'LIDO_Dortmund',
    'math.wisc.edu': 'MATH_WISC',
    'mwt2.org': 'MWT2',
    'nut.bu.edu': 'Boston',
    'palmetto.clemson.edu': 'Clemson-Palmetto',
    'panther.net': 'FLTECH',
    'phys.uconn.edu': 'UConn-OSG',
    'rcac.purdue.edu': 'Purdue-Hadoop',
    'research.northwestern.edu': 'NUMEP-OSG',
    'stat.wisc.edu': 'CHTC',
    't2.ucsd.edu': 'UCSDT2',
    'tier3.ucdavis.edu':'UCD',
    'unl.edu': 'Nebraska',
    'uppmax.uu.se': 'Uppsala',
    'usatlas.bnl.gov': 'BNL-ATLAS',
    'wisc.cloudlab.us': 'CLOUD_WISC',
    'wisc.edu': 'WISC',
    'zeuthen.desy.de': 'DESY-ZN',
}

def get_domain(hostname):
    parts = hostname.lower().split('.')
    if '.'.join(parts[-3:]) in reserved_domains:
        return '.'.join(parts[-3:])
    elif '.'.join(parts[-2:]) in reserved_domains:
        return '.'.join(parts[-2:])
    if len(parts) > 3:
        return '.'.join(parts[-3:])
    elif len(parts) > 2:
        return '.'.join(parts[-2:])

def get_ip_range(ip, reserved=False):
    parts = ip.split('.')
    if len(parts) == 4 and all(p.isdigit() for p in parts):
        if '.'.join(parts[:2]) in reserved_ips:
            ret = reserved_ips['.'.join(parts[:2])]
            if ret and reserved:
                return ret
            else:
                return None
        return '.'.join(parts[:3])

# do initial lookup
domain_lookup = {}
ip_lookup = {}
for row in db.condor_history.find(filter={'MATCH_EXP_JOBGLIDEIN_ResourceName':{'$exists': True}},
                                  projection=['LastRemoteHost','StartdPrincipal','MATCH_EXP_JOBGLIDEIN_ResourceName']):
    site = row['MATCH_EXP_JOBGLIDEIN_ResourceName']
    if '.' in site or site.startswith('gzk'):
        continue
    if site == 'Local Job' and 'LastRemoteHost' in row:
        domain = get_domain(row['LastRemoteHost'].split('@')[-1])
        if not domain:
            continue
        if domain.endswith('chtc.wisc.edu'):
            site = 'CHTC'
        elif domain.endswith('hep.wisc.edu'):
            site = 'HEP_WISC'
        elif domain.endswith('cs.wisc.edu'):
            site = 'CS_WISC'
        elif domain.endswith('math.wisc.edu'):
            site = 'MATH_WISC'
        elif domain.endswith('icecube.wisc.edu'):
            site = 'NPX'
        elif domain.endswith('wisc.edu'):
            site = 'WISC'
        elif domain.endswith('wisc.cloudlab.us'):
            site = 'CLOUD_WISC'
        else:
            print('Local Job with strange domain:',domain)
            continue
    if 'LastRemoteHost' in row:
        domain = get_domain(row['LastRemoteHost'].split('@')[-1])
        if domain:
            if domain in reserved_domains:
                continue
            if domain in domain_lookup and domain_lookup[domain] != site:
                print('duplicate site name for domain',domain)
                print(domain_lookup[domain])
                print(site)
            else:
                domain_lookup[domain] = site
    if 'StartdPrincipal' in row:
        ip_range = get_ip_range(row['StartdPrincipal'].split('/')[-1])
        if ip_range:
            if ip_range in ip_lookup and ip_lookup[ip_range] != site:
                print('duplicate site name for ip_range',ip_range)
                print(ip_lookup[ip_range])
                print(site)
            else:
                ip_lookup[ip_range] = site

def fix_site(row):
    site = row['MATCH_EXP_JOBGLIDEIN_ResourceName']
    new_site = None
    if site.startswith('gzk'):
        new_site = 'CHTC'
    if site.endswith('chtc.wisc.edu'):
        new_site = 'CHTC'
    elif site.endswith('hep.wisc.edu'):
        new_site = 'HEP_WISC'
    elif site.endswith('cs.wisc.edu'):
        new_site = 'CS_WISC'
    elif site.endswith('math.wisc.edu'):
        new_site = 'MATH_WISC'
    elif site.endswith('icecube.wisc.edu'):
        new_site = 'NPX'
    elif site.endswith('wisc.cloudlab.us'):
        new_site = 'CLOUD_WISC'
    elif site.endswith('wisc.edu'):
        new_site = 'WISC'
    elif '.' not in site and site != 'Local Job':
        return # fine the way it is
    if new_site:
        filter = {'GlobalJobId':row['GlobalJobId']}
        update = {'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':new_site}}
        db.condor_history.update_one(filter, update)
        return
    raise Exception('cannot fix')

def fix_host(row):
    host = row['LastRemoteHost'].split('@')[-1]
    domain = get_domain(host)
    if domain and domain in reserved_domains:
        filter = {'GlobalJobId':row['GlobalJobId']}
        update = {'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':reserved_domains[domain]}}
        db.condor_history.update_one(filter, update)
        return
    if domain and domain in domain_lookup:
        filter = {'GlobalJobId':row['GlobalJobId']}
        update = {'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':domain_lookup[domain]}}
        db.condor_history.update_one(filter, update)
        return
    print('unknown domain',domain)
    raise Exception('cannot fix')

def fix_ip(row):
    ip = row['StartdPrincipal'].split('/')[-1]
    ip_range = get_ip_range(ip, reserved=True)
    if not ip_range:
        raise Exception('reserved ip')
    if '.' not in ip_range: # reserved range
        filter = {'GlobalJobId':row['GlobalJobId']}
        update = {'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':ip_range}}
        db.condor_history.update_one(filter, update)
        return
    if ip_range and ip_range in ip_lookup:
        filter = {'GlobalJobId':row['GlobalJobId']}
        update = {'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':ip_lookup[ip_range]}}
        db.condor_history.update_one(filter, update)
        return
    print('unknown ip',ip_range)
    raise Exception('cannot fix')


# remap bad sites
for row in db.condor_history.find(projection=['GlobalJobId','LastRemoteHost','StartdPrincipal','MATCH_EXP_JOBGLIDEIN_ResourceName']):
    if 'MATCH_EXP_JOBGLIDEIN_ResourceName' in row:
        try:
            fix_site(row)
        except:
            pass
        else:
            continue
    if 'LastRemoteHost' in row:
        try:
            fix_host(row)
        except:
            pass
        else:
            continue
    if 'StartdPrincipal' in row:
        try:
            fix_ip(row)
        except:
            pass
        else:
            continue
    print('cannot fix')
