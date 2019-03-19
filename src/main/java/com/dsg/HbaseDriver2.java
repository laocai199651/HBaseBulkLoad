package com.dsg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HbaseDriver2 extends Configuration implements Tool {
    private Configuration configuration = null;

    public static void main(String[] args) throws Exception {
        /*Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        conf.setInt("epoch.seconds.tipoff", 1275613200);
        conf.set("hbase.table.name", args[2]);

        // Load hbase-site.xml
        HBaseConfiguration.addHbaseResources(conf);

        Job job = new Job(conf, "HBase Bulk Import Example");
        job.setJarByClass(HbaseKVMapper.class);

        job.setMapperClass(HbaseKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setInputFormatClass(TextInputFormat.class);

        HTable hTable = new HTable(conf,args[2]);

        // Auto configure partitioner and reducer
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);*/


        Configuration configuration = HBaseConfiguration.create();
        //配置zk主机
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "hadoop101");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        //坑
        //configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
        System.out.println("cwklog:================ HConstants.ZOOKEEPER_QUORUM   " + configuration.get(HConstants.ZOOKEEPER_QUORUM));
        System.out.println("cwklog:================ HConstants.ZOOKEEPER_ZNODE_PARENT   " + configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
        int i = ToolRunner.run(configuration, new HbaseDriver2(), args);
        System.exit(i);

    }

    @Override
    public int run(String[] args) throws Exception {

        final String INPUT_PATH = args[0];
        final String OUTPUT_PATH = args[1];
        //Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Table table = connection.getTable(TableName.valueOf(args[2]));
        Job job = Job.getInstance(configuration);
        //job.getConfiguration().set("mapred.jar", "/home/hadoop/TravelProject/out/artifacts/Travel/Travel.jar");  //预先将程序打包再将jar分发到集群上
        job.setJarByClass(HbaseDriver2.class);
        job.setMapperClass(HbaseKVMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.configureIncrementalLoad(job, table, connection.getRegionLocator(TableName.valueOf(args[2])));
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
