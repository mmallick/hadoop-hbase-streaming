/*
 * HBase OutputFormat for Hadoop Streaming.
 * Only for insert data.
 *
 * Format:
 * <RowID>	<ColumnName>	<value>	(<timestamp>)
 *
 * Options:
 * -jobconf reduce.output.table=<TableName>
 * -jobconf reduce.output.binary=<true|false> (default: false)
 * -jobconf stream.reduce.output.field.separator=<Separator> (default: tab)
 *
 * Notice:
 * Run streaming with dammy -output option.
 */

package org.childtv.hadoop.hbase.mapred;

import java.util.Date;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.io.Writable;

public class PutTableOutputFormat extends TextTableOutputFormat {

    // defined at org.apache.hadoop.streaming.PipeMapRed
    public static final String SEPARATOR_KEY = "stream.reduce.output.field.separator";
    public static final String DEFAULT_SEPARATOR = "\t";

    private String separator;

    public void configure(JobConf job) {
        super.configure(job);
        separator = job.get(SEPARATOR_KEY);
        if (separator == null)
            separator = DEFAULT_SEPARATOR;
    }

    public Writable[] createBatchUpdates(String key, String content) {
        String[] values = content.split(separator, 3);
        if (values.length < 3)
            throw new RuntimeException("PutTableOutputFormat: invalid reduce output: " + content);

        
        Put bu = new Put(decodeValue(key));
        
        if (values.length >= 4) {
        	try {
        	bu.add(decodeColumnName(values[0]), decodeColumnName(values[1]), Long.parseLong(values[3]),  decodeValue(values[2]));
           
            } catch(NumberFormatException e) {}
        } else {
        	bu.add(decodeColumnName(values[0]), decodeColumnName(values[1]), new Date().getTime(),  decodeValue(values[2]));
        }
        
        return new Writable[] { bu };
    }

}
