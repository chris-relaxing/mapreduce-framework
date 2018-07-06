#!/usr/lib/python_2.7.3/bin/python

import sys
import codecs

sys.stdout = codecs.getwriter('utf-8')(sys.stdout)
inData = codecs.getreader('utf-8')(sys.stdin)

(last_key, tot_cnt, v_id) = (None, 0, None)

for line in inData:
    try:
        (key, val) = line.strip().split("\t")
        if last_key != key:
            if last_key != None:
                k = last_key.split('|')
                v_id = k[0]

                if v_id == 'Basic_0017a':
                    if tot_cnt > 1:
                        sys.stdout.write("%s\t%s\n" % (last_key,tot_cnt))
                else:
                    sys.stdout.write("%s\t%s\n" % (last_key,tot_cnt))

            (last_key, tot_cnt) = (key, int(val))
        else:
            (last_key, tot_cnt) = (key, tot_cnt + int(val))
    except:
        pass

if last_key:
    if v_id == 'Basic_0017a':
        if tot_cnt > 1:
            sys.stdout.write("%s\t%s\n" % (last_key, tot_cnt))
    else:
        sys.stdout.write("%s\t%s\n" % (last_key, tot_cnt))



