package com.dsg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {

    final static byte[] SRV_COL_FAM = "srv".getBytes();
    final static int NUM_FIELDS = 16;


    ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    KeyValue kv;

    /** {@inheritDoc} */
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
       /* Configuration c = context.getConfiguration();

        tipOffSeconds = c.getInt("epoch.seconds.tipoff", 0);
        tableName = c.get("hbase.table.name");*/
    }

    /** {@inheritDoc} */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {





    }
}
