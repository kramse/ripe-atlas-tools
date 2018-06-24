#!/usr/bin/env python
import sys
import bz2
import ujson as json
from influxdb import InfluxDBClient

### test code for importing ripe atlas traceroutes
### into influxdb for anomaly detection

passwd = sys.argv[1] # so i don't put it in the github!
source_data = sys.argv[2] # bz file from RIPE Atlas 

client = InfluxDBClient(host='s001.ams.scaleway.infra.hayes.ie', port=8086, username='hackathon', password=passwd, database='rtts')
client.create_database('rtts')

'''
payment,device=mobile,product=Notepad,method=credit billed=33,licenses=3i 1434067467100293230
rtts,ip={IP},prb_id={PRB_ID} rtt={RTT} {TIMESTAMP}
'''

source_file = bz2.BZ2File(sys.argv[2], "r")
for line in source_file:
	d = json.loads( line )
	if 'result' in d:
			vals = []
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
								client.write(['rtts,ip="{IP}",prb_id={PRB_ID} value={RTT} {TIMESTAMP}'.format(
													IP=ip, PRB_ID=d['prb_id'], RTT=rtt, TIMESTAMP=d['timestamp']
													)],
												 {'db':'rtts', 'precision': 's'},204,'line')
