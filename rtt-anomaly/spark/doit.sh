HDFSDIR_BASE='hdfs://ursa/user/eaben/prb-ip-95pct/'
FNAME_BASE='prb-ip-95pct'
DDATE=2018-06-22

hadoop fs -rm -r $HDFSDIR_BASE/$DDATE
time /data/spark/bin/spark-submit --master yarn --executor-memory 7G --deploy-mode client --num-executors 70  --conf spark.yarn.executor.memoryOverhead=1024 ./extract.py $DDATE $HDFSDIR_BASE
hadoop fs -cat $HDFSDIR_BASE/$DDATE/part-* | bzip2 > $FNAME_BASE.$DDATE.bz2
hadoop fs -rm -r $HDFSDIR_BASE/$DDATE
