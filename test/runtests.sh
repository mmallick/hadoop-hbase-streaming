#!/bin/sh

TEST_BASE_DIR=$(dirname $0)
TABLE_NAME="test_table_$$"
TMP_DIR="tmp_$$"
TEST_OUTPUT_DIR="$TMP_DIR/test_output"
STREAMING_JAR=/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.0.1.jar

function create_tmp_dir(){
	mkdir $TMP_DIR
}
function remove_tmp_dir(){
	rm -rf $TMP_DIR
}

function create_table(){
	hbase shell << EOF
		create "$TABLE_NAME", {NAME=>'cf1'}, {NAME=>'cf2'}
EOF
}

function load_sample_data(){
	echo "Loading Sample Data"

	hadoop jar $STREAMING_JAR \
		-fs file:/// -jt local \
		-input input_files -output $TMP_DIR/dummy_output \
		-mapper /bin/cat \
		-outputformat org.childtv.hadoop.hbase.mapred.ListTableOutputFormat \
		-jobconf reduce.output.table=$TABLE_NAME
}


function extract_json(){
	echo "Extracting sample using JSONTableInputFormat"

	hadoop jar $STREAMING_JAR \
		-fs file:/// -jt local \
		-input dummy_input \
		-inputformat org.childtv.hadoop.hbase.mapred.JSONTableInputFormat \
		-mapper /bin/cat -jobconf map.input.table=$TABLE_NAME \
		-jobconf map.input.columns="cf1 cf2" -output $TEST_OUTPUT_DIR

}

function drop_table(){
	hbase shell << EOF
		disable "$TABLE_NAME"
		drop "$TABLE_NAME"
EOF
}

function show_diff(){
	echo "Displaying difference between test and expected data: "
	echo
	diff expected_output $TEST_OUTPUT_DIR/part-00000
	if [ $? -ne 0 ]
	then
		echo "FAILED: Expected output differs."
		echo "Check $TEST_OUTPUT_DIR for output."
		exit 1
	else
		echo "SUCCESS"
	fi

}

pushd $TEST_BASE_DIR > /dev/null

create_tmp_dir
create_table
load_sample_data
extract_json
drop_table
show_diff
remove_tmp_dir

popd > /dev/null

exit 0
