import glob
import os

LOC='/reg/d/ana01/temp/davidsch/lc2/runA'

logs = glob.glob(os.path.join(LOC, 'logs', '*.log'))

records = []
for log in logs:
    lines = open(log,'r').read().split('\n')
    last_hdr_base = '1492100000000'
    last_hdr_number = 0

    for ln in lines:
        flds = ln.split()
        if len(flds)==0:
            continue
        if flds[0].startswith('149'):
            next_last_hdr_base = ' '.join([flds[0],flds[1]])
            if next_last_hdr_base == last_hdr_base:
                last_hdr_number += 1
            else:
                last_hdr_base = next_last_hdr_base
                last_hdr_number = 0
            ln = last_hdr_base + str(last_hdr_number) + ' ' + ' '.join(flds[2:])
        else:
            last_hdr_number += 1
            ln = last_hdr_base + str(last_hdr_number) + ' ' + ln
        records.append(ln)
                           
records.sort()
print ('\n'.join(records))
