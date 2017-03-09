#!/usr/bin/python

import os
import subprocess
from xml.etree import ElementTree

def get_rev_date(url, revision):
    urls = [url]
    u2 = url
    u = os.path.dirname(url)
    while u != u2:
        urls.append(u)
        u2 = u
        u = os.path.dirname(u)

    for u in urls:
        try:
            output = subprocess.check_output(['svn','log','--limit','1','-r',revision,u,'--xml'])
            root = ElementTree.fromstring(output)
            e = root.findall('.//date')
            if not e:
                continue
            return e[0].text
        except Exception as e:
            pass
    raise Exception('cannot find revision date')

def get_externals(url, revision):
    ext = {}
    output = subprocess.check_output(['svn','propget','svn:externals','-r',revision,url])
    for line in output.split('\n'):
        if '#' in line:
            line = line[:line.find('#')]
        line = line.strip()
        if not line:
            continue
        parts = line.split(' ',1)
        if len(parts) > 1:
            ext[parts[0].strip()] = parts[1].strip()
    return ext

def svn_checkout(url, path, revision, depth='infinity', cwd=None):
    subprocess.check_call(['svn','co','--depth',depth,'-r',revision,url,path],cwd=cwd)

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Download with svn externals = revision number')
    parser.add_argument('-r', '--revision', default=-1, help='revision number')
    parser.add_argument('url', nargs=1, help='url to svn containing svn externals')
    parser.add_argument('path', nargs='?', default=None, help='output directory')
    args = parser.parse_args()

    url = args.url[0]

    if args.revision == -1:
        raise Exception('must specify revision')
    try:
        int(args.revision)
    except:
        # revision is date
        revision = args.revision
    else:
        # revision is number
        revision = '{'+get_rev_date(url, args.revision)+'}'

    if not args.path:
        path = os.path.basename(args.url)
    else:
        path = args.path

    ext = get_externals(url, revision)
    if not ext:
        raise Exception('could not get externals')
    svn_checkout(url, path, revision, depth='immediates')
    for e in ext:
        svn_checkout(ext[e], e, revision, cwd=path)

if __name__ == '__main__':
    main()
