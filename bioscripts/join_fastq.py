import gzip
import sys
from itertools import izip

samp_name = "SP-4h-Rep2"
fq1 = "SP-4h-Rep2_1_tr.fq.gz"
fq2 = "SP-4h-Rep2_2_tr.fq.gz"
test = 1
record_ind = 1
of = gzip.open(samp_name + "_join.fq.gz", 'wb')
with gzip.open(fq1, 'rb') as f1, gzip.open(fq2, 'rb') as f2:
    for line1, line2 in izip(f1, f2):
        if record_ind == 1:
            try:
                line1 == line2
                record_ind += 1
                of.write(line1)
            except:
                print "Error !!! read name mismatch"
                sys.exit(0)
        elif record_ind == 2:
            # print "%s" %( line1.strip('\n') + " + " + line2)
            # print line1.strip('\n') + line2
            of.write(line1.strip('\n') + line2)
            record_ind += 1
        elif record_ind == 3:
            # print line1.strip('\n'), line2
            of.write(line1.strip('\n') + line2)
            record_ind += 1
        else:
            # print line1.strip('\n') + " + " + line2
            # print line1.strip('\n') + line2
            of.write(line1.strip('\n') + line2)
            record_ind = 1

    of.close()
    f1.close()
    f2.close()
    # test +=1
    # if test > 20:
    #    sys.exit(0)
