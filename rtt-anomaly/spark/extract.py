import ujson as json
import sys
import math
import re
from collections import Counter
from operator import add
from pyspark.context import SparkContext
sc = SparkContext()
import sys
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

DAY=sys.argv[1]
HDFS_LOC=sys.argv[2]

file_format="org.apache.hadoop.mapred.SequenceFileInputFormat"
key_class="org.apache.hadoop.io.Text"
value_class="org.apache.hadoop.io.Text"

def mp_ip_rtts( i ):
    '''
    input: raw atlas traceroute
    output: datafram of (prb,ip), and rtt tuples
    '''
    # {"af": 4, "prb_id": 10052, "result": [{"rtt": 7.78455}, {"rtt": 7.73801}, {"rtt": 7.735605}], "ttl": 55, "avg": 7.7527216667, "size": 20, "from": "95.130.22.90", "proto": "ICMP", "timestamp": 1483228844, "dup": 0, "type": "ping", "sent": 3, "msm_id": 1001, "fw": 4740, "max": 7.78455, "step": 240, "src_addr": "95.130.22.90", "rcvd": 3, "msm_name": "Ping", "lts": 72, "dst_name": "193.0.14.129", "min": 7.735605, "dst_addr": "193.0.14.129"}
    out = {}
    #key = (row['prb_id'],row['dst_addr'])
    #key = "%s|%s" % (row['prb_id'], row['dst_addr'])
    ## array: 0: hops with responses  1: hops without responses
    for d in i:
        if 'result' in d:
            if not 'prb_id' in d or not 'dst_addr' in d:
                continue
            for hr in d['result']:
                if 'result' in hr:
                    for h in hr['result']:
                        if 'edst' in h:
                            # doesn't belong in this trace
                            continue
                        if 'from' in h and 'rtt' in h:
                            ip = h['from']
                            rtt = h['rtt']
                            key = (d['prb_id'],ip)
                            out.setdefault( key, [] )
                            out[key].append( rtt )
    return out.iteritems()

## {{{ http://code.activestate.com/recipes/511478/ (r1)

def percentile(N, percent, key=lambda x:x):
    """
    Find the percentile of a list of values.

    @parameter N - is a list of values. Note N MUST BE already sorted.
    @parameter percent - a float value from 0.0 to 1.0.
    @parameter key - optional key function to compute value from each element of N.

    @return - the percentile of the values
    """
    if not N:
        return None
    k = (len(N)-1) * percent
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return key(N[int(k)])
    d0 = key(N[int(f)]) * (c-k)
    d1 = key(N[int(c)]) * (k-f)
    return d0+d1


def map_cnt_pct( d ):
    ## d = 
    cnt = len( d )
    d.sort()
    p0 = d[0]
    p95 = percentile(d,0.95)
    p99 = percentile(d,0.99)
    return (cnt,p0,p95,p99)

def rbk_extend( a, b):
    out = []
    if type(a) == list:
        out.extend( a )
    else:
        print "AAA: %s (%s)" % (a, type(a) )
    if type(b) == list:
        out.extend( b )
    else:
        print "AAA: %s (%s)" % (b, type(b) )
    return out

trace_path="/raw/atlas/day/type=traceroute/%s.seq" % ( DAY )

# load traceroute
#t1 = sc.hadoopFile(trace_path, file_format, key_class, value_class).sample(False,0.005)
t1 = sc.hadoopFile(trace_path, file_format, key_class, value_class)
t2 = t1.map( lambda v: json.loads(v[1]) )
t2plus = t2.coalesce(70,shuffle=False)

# finds all RTTs for (src,dst) combinations in a partition
t3 = t2plus.mapPartitions( mp_ip_rtts )

#t4 = t3.reduceByKey( lambda a,b: a.extend(b) )
t4 = t3.reduceByKey( rbk_extend )

# now we have [[prb_id,ip],[list_of_rtts] ]
t5 = t4.mapValues( map_cnt_pct )

t5.map( lambda x: json.dumps(x) ).saveAsTextFile("%s/%s" % (HDFS_LOC, DAY) )
