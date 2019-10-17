#!/usr/bin/env python3
"""
Find all regular users logged in on normal systems.
"""
import subprocess
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-s','--skip',type=int,default=0)
args = parser.parse_args()

machines = ["sub-1","pub","submit","submit-sl6"]
machines.extend(f"cobalt0{i}" for i in range(1,9))

uid_to_username = {}
def get_username(uid):
    global uid_to_username
    if uid in uid_to_username:
        return uid_to_username[uid]
    output = subprocess.check_output(['ssh','cobalt01',f'getent passwd {uid}']).decode('utf-8')
    user = output.split(':')[0]
    uid_to_username[uid] = user
    return user

def find_logged_in(machine):
    for line in subprocess.check_output(['ssh',machine,'ps --no-headers -eo user:50,uid']).decode('utf-8').split('\n'):
        line = line.strip()
        if line:
            parts = line.split()
            if int(parts[1]) > 1005:
                if parts[0].isnumeric():
                    yield get_username(parts[0])
                else:
                    yield parts[0]

def find_condor_jobs(machine):
    for line in subprocess.check_output(['ssh',machine,'condor_q -all -af Owner']).decode('utf-8').split('\n'):
        line = line.strip()
        if line:
            yield line

def is_moved(user):
    try:
        output = subprocess.check_output(['ssh','cobalt',f'ls -l /mnt/lfs3/user/{user}']).decode('utf-8').split('\n')
    except Exception:
        return True
    if output[0].startswith('lrwx') and '->' in output[0]:
        return True
    else:
        return False

def get_users_to_move():
    for line in subprocess.check_output(['ssh','cobalt','ls -l /mnt/lfs3/user/']).decode('utf-8').split('\n'):
        line = line.strip()
        if line.startswith('lrwx') and '->' in line:
            continue
        if line.startswith('drwx'):
            yield line.split()[-1]

def get_quota_size(user):
    for line in subprocess.check_output(['ssh','cobalt',f'sudo lfs quota -u {user} /mnt/lfs3'],stderr=subprocess.DEVNULL).decode('utf-8').split('\n'):
        if '/mnt/lfs3' in line:
            parts = line.strip().split()
            return {'size':int(parts[1]),'files':int(parts[5])}

users = set()
for m in machines:
    users.update(find_logged_in(m))
    if m.startswith('sub'):
        users.update(find_condor_jobs(m))

print(sorted(users))
print('num users logged in:',len(users))

users_to_move = set(get_users_to_move())
users_moving_now = users_to_move-users
users_delayed = users_to_move-users_moving_now

print('Users delayed:',len(users_delayed))
print(sorted(users_delayed))

print('Users to move:',len(users_moving_now))
total_size = 0
total_files = 0
for u in sorted(users_moving_now):
    if args.skip > 0:
        args.skip -= 1
        continue
    print(u,end=' ')
    try:
        sizes = get_quota_size(u)
    except Exception:
        pass
    else:
        total_size += sizes['size']
        total_files += sizes['files']
        if (total_size > 1000000000 # 1 TB
            or total_files > 500000): # 500K
            print('\n...')
            break

