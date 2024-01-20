package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class InvertedIndexDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1. 创建HBase配置conf
        Configuration conf = HBaseConfiguration.create();

        // 2. 设置调大客户端和服务端参数为55MB，避免当实际插入的keyValue的大小超过默认大小10MB限制阈值时，发生报错
        conf.set("hbase.client.keyvalue.maxsize", "57671680");
        conf.set("hbase.server.keyvalue.maxsize", "57671680");

        // 3. 辨别命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // 4. 获取job
        Job job = Job.getInstance(conf);

        // 5. 设置jar包路径
        job.setJarByClass(InvertedIndexDriver.class);

        // 6. 关联mapper和reducer
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // 7. 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 8. 设置读取输入文件的格式
        // 如果不设置InputFormat，默认使用的是TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);

        // 9. 虚拟存储切片最大值设置为12MB
        CombineTextInputFormat.setMaxInputSplitSize(job, 12582912);

        // 10. 根据首字符分区存储
        job.setPartitionerClass(Partition.class);
        job.setNumReduceTasks(36);

        // 11. 设置结果输出到HBase表
        // "bingh:bigdata"是已经在HBase中建好的表
        TableMapReduceUtil.initTableReducerJob("bingh:inverted", InvertedIndexReducer.class, job, Partition.class);

        // 12. 设置输入路径
        // 这是上传到HDFS上多个小文件的父目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        // 13. 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }

}
