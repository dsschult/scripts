"""
Read files from apache cache and reconstruct them
"""

import os
import shutil

def get_filename(path, server_name):
    """Get the filename from a .header file"""
    with open(path, 'r') as f:
        data = f.read()
    ret = data.split(server_name)[1].split()[0].split('?')[0]
    if ret.endswith('/'):
        raise Exception('directory entry')
    return ret.strip('/')

def copy(in_path, out_path):
    print(in_path, out_path)
    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))
    shutil.copyfile(in_path, out_path)

def main():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Reconstruct Apache mod-disk-cache entries')
    parser.add_argument('-i','--input',help='cache dir')
    parser.add_argument('-o','--output',help='output dir')
    parser.add_argument('--server_name',default='prod-exe.icecube.wisc.edu:80',help='server name to help search')
    args = parser.parse_args()
    
    for root,dirs,files in os.walk(args.input):
        for f in files:
            if f.endswith('.header'):
                try:
                    path = get_filename(os.path.join(root, f), args.server_name)
                except Exception:
                    continue
                in_path = os.path.join(root, f.replace('.header', '.data'))
                out_path = os.path.join(args.output, path)
                copy(in_path, out_path)

if __name__ == '__main__':
    main()

