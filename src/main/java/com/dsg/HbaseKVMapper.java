package com.dsg;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HbaseKVMapper extends
        Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

   // final static byte[] SRV_COL_FAM = "srv".getBytes();
   // final static int NUM_FIELDS = 16;


    //ImmutableBytesWritable hKey = new ImmutableBytesWritable();
    //KeyValue kv;

    /** {@inheritDoc} */
  /*  @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
       *//* Configuration c = context.getConfiguration();

        tipOffSeconds = c.getInt("epoch.seconds.tipoff", 0);
        tableName = c.get("hbase.table.name");*//*
    }*/

    /** {@inheritDoc} */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //获取一行数据

        String line = value.toString();

        //切割
        //0 => rowkey 1=>cf1:cn1 2=>value1 3=>cf1:cn2 4=>value2 5=>cf2:cn1 6=>value3 7=>cf2:cn2 8=>value4
        String[] split = line.split("\t");

        //封装Put对象

        Put put = new Put(Bytes.toBytes(split[0]));

        put.addColumn(Bytes.toBytes(split[1].split(":")[0]), Bytes.toBytes(split[1].split(":")[1]), Bytes.toBytes(split[2]))
                .addColumn(Bytes.toBytes(split[3].split(":")[0]), Bytes.toBytes(split[3].split(":")[1]), Bytes.toBytes(split[4]))
                .addColumn(Bytes.toBytes(split[5].split(":")[0]), Bytes.toBytes(split[5].split(":")[1]), Bytes.toBytes(split[6]))
                .addColumn(Bytes.toBytes(split[7].split(":")[0]), Bytes.toBytes(split[7].split(":")[1]), Bytes.toBytes(split[8]));

        ImmutableBytesWritable rowkey = new ImmutableBytesWritable(Bytes.toBytes(split[0]));
        //当导入的数据有多列时使用Put，只有一个列时使用KeyValue
        //KeyValue kv = new KeyValue(Bytes.toBytes(row), this.CF_KQ.getBytes(), datas[1].getBytes(), datas[2].getBytes());
        context.write(rowkey, put);



    }
}
