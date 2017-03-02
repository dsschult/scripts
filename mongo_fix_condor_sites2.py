from __future__ import print_function

from pymongo import MongoClient

db = MongoClient('mongodb-condor.icecube.wisc.edu').condor

reserved_ips = {
    '192.168': None,
    '192.84': 'Ultralight',
    '192.41': 'AGLT2',
}
reserved_ips.update({'10.%d'%i:None for i in range(256)})
reserved_ips.update({'172.%d'%i:None for i in range(16,32)})

reserved_domains = {
    'aglt2.org': 'AGLT2',
}

def get_domain(hostname):
    parts = hostname.split('.')
    if len(parts) > 2:
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
    raise Exception('cannot fix')


# remap bad sites
raise Exception()
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
