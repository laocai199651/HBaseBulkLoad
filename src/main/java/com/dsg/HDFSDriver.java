package com.dsg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HDFSDriver extends Configuration implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {

        //获取job对象
        Job job = Job.getInstance(configuration);

        //设置主类
        job.setJarByClass(HDFSDriver.class);
        //设置Mapper
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);

        //设置Reducer
        TableMapReduceUtil.initTableReducerJob(args[0], HDFSReducer.class, job);

        //设置输入路径
        FileInputFormat.setInputPaths(job, args[1]);

        //提交
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        //配置zk主机
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "hadoop101");
        configuration.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        //坑
        //configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase-unsecure");
        System.out.println("cwklog:================ HConstants.ZOOKEEPER_QUORUM   "+configuration.get(HConstants.ZOOKEEPER_QUORUM));
        System.out.println("cwklog:================ HConstants.ZOOKEEPER_ZNODE_PARENT   "+configuration.get(HConstants.ZOOKEEPER_ZNODE_PARENT));
        int i = ToolRunner.run(configuration, new HDFSDriver(), args);
        System.exit(i);
    }


}
