from pymongo import MongoClient

db = MongoClient('mongodb-condor.icecube.wisc.edu').condor

sites = {}
fixup = {}
for row in db.condor_history.find(projection=['GlobalJobId','LastRemoteHost','StartdPrincipal','MATCH_EXP_JOBGLIDEIN_ResourceName']):
    if 'LastRemoteHost' not in row:
        continue
    host = row['LastRemoteHost'].split('@')[-1]
    resource_name = row['MATCH_EXP_JOBGLIDEIN_ResourceName'] if 'MATCH_EXP_JOBGLIDEIN_ResourceName' in row else ''
    if '.' in resource_name or 'gzk' in resource_name:
        resource_name = None
    if not resource_name:
        if host not in fixup:
            fixup[host] = []
        fixup[host].append(row['GlobalJobId'])
        continue
    s = row['MATCH_EXP_JOBGLIDEIN_ResourceName']
    if s not in sites:
        sites[s] = set()
    sites[s].add(host)

print('done finding, now update')
updates = {s:[] for s in sites}
for host in fixup:
    for s in sites:
        if host in sites[s]:
            updates[s].extend(fixup[host])
            break
for s in updates:
    for uid in updates[s]:
        db.condor_history.update_one({'GlobalJobId':uid},{'$set':{'MATCH_EXP_JOBGLIDEIN_ResourceName':s}})
