Initial Setup
=============

All instructions pertain to CDH3u3 on CentOS 6.

Place hadoop-hbase-streaming.jar in /usr/local/hadoop-hbase-streaming.jar

Add to :  /etc/hadoop-0.20/conf/hadoop-env.sh

	export HADOOP_CLASSPATH="/usr/local/hadoop-hbase-streaming.jar:$HADOOP_CLASSPATH"
	export HADOOP_CLASSPATH="/usr/lib/hbase/lib/guava-r06.jar:$HADOOP_CLASSPATH"
	export HADOOP_CLASSPATH="/usr/lib/hbase/hbase-0.90.4-cdh3u3.jar:$HADOOP_CLASSPATH"
	export HADOOP_CLASSPATH="/usr/lib/zookeeper/zookeeper-3.3.4-cdh3u3.jar:$HADOOP_CLASSPATH" 

Loading Data into HBase
=======================

Create the output table with appropriate column families:

	create 'outputtable', {NAME=>'cf1'}, {NAME=>'cf2'}

Create a reducer that will output in the following format (tab-delimited):

	put	<rowid>	<cf>	<qualifier>	<value>

Run your map reduce job with the OutputFormat set to: org.childtv.hadoop.hbase.mapred.ListTableOutputFormat

As a test, create a file called source_input/test.tab and include the expected reducer output.  

An example of the reducer output might be (tab-delimited): 

	put	r1	cf1	test	Value1
	put	r1	cf2	test	Value2
	put	r2	cf1	test	Value3 

Then invoke the hadoop streaming API with the outputformat set to  org.childtv.hadoop.hbase.mapred.ListTableOutputFormat and the job configuration parameter reduce.output.table=outputtable 

	hadoop jar  /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u3.jar \
		-input source_input -output dummy_output \
		-mapper /bin/cat \
		-outputformat org.childtv.hadoop.hbase.mapred.ListTableOutputFormat \
		-jobconf reduce.output.table=outputtable 

This will write the provided fields to HBase.


Extracting Data from HBase
==========================

For reading from hbase, create a dummy input directory containing no files.

	mkdir dummy_input

Select your desired InputFormat.  Two exist :
JSON: org.childtv.hadoop.hbase.mapred.JSONTableInputFormat
Tabular values: org.childtv.hadoop.hbase.mapred.ListTableInputFormat

Select you desired input column families using the job configuration parameter map.input.columns

The JSON format has the advantage that the format is stricter and more expressive.  

	r1	{"cf2:test":{"timestamp":"1333428648468","value":"Value1"},"cf1:test":{"timestamp":"1333428678724","value":"Value2"}} 
	r2	{"cf2:test":{"timestamp":"1333428656033","value":"Value3"},"cf1:test":{"timestamp":"1333428660721","value":"Value4"}} 

The ListTableInputFormat only includes rowid and value.  It does not include column names in any way.

	r1	Value1 Value2
	r2	Value3 Value4

To run a test job on an HBase table called sourcetable with column families cf1 and cf2 and run:

	hadoop jar /usr/lib/hadoop-0.20/contrib/streaming/hadoop-streaming-0.20.2-cdh3u3.jar \
		-input dummy_input -inputformat org.childtv.hadoop.hbase.mapred.JSONTableInputFormat \
		-mapper /bin/cat \
		-jobconf map.input.table=sourcetable -jobconf "map.input.columns=cf1 cf2" \
		-output myoutput 

This will produce a file in myoutput/part-00000 that contains the JSON output.

