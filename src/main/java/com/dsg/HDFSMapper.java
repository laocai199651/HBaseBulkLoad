package com.dsg;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HDFSMapper extends Mapper<LongWritable, Text, NullWritable, Put> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

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


        //写出去
        context.write(NullWritable.get(), put);
    }
}
