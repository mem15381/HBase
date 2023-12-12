package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import java.io.IOException;

public class HbaseReadWrite extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(HBaseConfiguration.create(), new HbaseReadWrite(), args);
    }

    public int run(String[] args) throws Exception {

        Configuration config = HBaseConfiguration.create();
        TableName table = TableName.valueOf("bmi");
        Job job = new Job(config, "ExampleReadWrite");
        job.setJarByClass(HbaseReadWrite.class);    // class that contains mapper

        Scan scan = new Scan();
        scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // set other scan attrs

        TableMapReduceUtil.initTableMapperJob(
                table,      // input table
                scan,             // Scan instance to control CF and attribute selection
                MyMapper.class,   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job);
        TableMapReduceUtil.initTableReducerJob(
                "bmi",      // output table
                null,             // reducer class
                job);
        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);
        if (!b) {
            throw new IOException("error with job!");
        }
        return 0;
    }

    public static class MyMapper extends TableMapper<ImmutableBytesWritable, Put> {

        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
            // this example is just copying the data from the source table...
            context.write(row, resultToPut(row,value));
        }

        private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
            Put put = new Put(key.get());
            for (Cell cell : result.listCells()) {
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                byte[] value = CellUtil.cloneValue(cell);
                put.add(cell);
            }
            return put;
        }
    }
}
