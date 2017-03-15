#!/usr/bin/env python
import sys
import csv

out = sys.argv[-1]
header = []
data = {}

for infile in sys.argv[1:-1]:
    print('reading',infile)
    with open(infile, 'r') as csvfile:
        csvfilereader = csv.reader(csvfile)
        local_header = []
        for i,row in enumerate(csvfilereader):
            if i == 0: # header
                local_header = row[1:]
                if not header:
                    header = row[1:]
            else:
                id = row[0]
                row_data = map(float,row[1:])
                print(row_data)
                if id not in data:
                    data[id] = {c:d for c,d in zip(local_header,row_data)}
                else:
                    for c,d in zip(local_header,row_data):
                        if c in data[id]:
                            data[id][c] += d
                        else:
                            data[id][c] = d

print('writing',out)
with open(out, 'w') as outfile:
    outwriter = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
    outwriter.writerow(['site']+header)
    for id in sorted(data,key=lambda id:sum(data[id].values()),reverse=True):
        outwriter.writerow([id]+[data[id][c] if c in data[id] else 0 for c in header])
