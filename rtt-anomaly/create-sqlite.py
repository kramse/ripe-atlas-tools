#!/usr/bin/env python
import bz2
import sys
import sqlite3
import ujson as json
import os.path

### NOTE this is a proof-of-concept
## reads sys.argv[1] and imports into a sqlite

DBFILE='rtts.sqlite'

conn = None

if os.path.isfile( DBFILE ):
	print >>sys.stderr, "removing old DBFILE %s" % ( DBFILE )
	os.remove( DBFILE )
conn = sqlite3.connect( DBFILE )
c = conn.cursor()
c.execute('CREATE TABLE rtts (prb_id integer, ip string, pct95 float, pct99 float)')
conn.commit()

# now import data
i=0
p = []
source_file = bz2.BZ2File(sys.argv[1], "r")
for line in source_file:
	j = json.loads( line )
	cnt = j[1][0] # counts
	if cnt < 10:
		continue
	prb_id = j[0][0]
	ip = j[0][1]
	#min rtt is j[1][1]
	pct95 = j[1][2]
	pct99 = j[1][3]
	i+=1
	p.append([prb_id,ip,pct95,pct99])
	if not i % 20000:
		c.executemany('INSERT INTO rtts VALUES (?,?,?,?)', p)
		conn.commit()
		p=[]
		print >>sys.stderr, "inserted %s" % (i)

# last remainder
c.executemany('INSERT INTO rtts VALUES (?,?,?,?)', p)
conn.commit()
c.execute('CREATE INDEX rtts_idx on rtts (prb_id, ip)')
conn.commit()
