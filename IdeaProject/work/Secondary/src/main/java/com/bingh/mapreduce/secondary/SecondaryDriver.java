package com.bingh.mapreduce.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SecondaryDriver{
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        //获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //设置jar包路径
        job.setJarByClass(SecondaryDriver.class);

        //关联mapper和reducer
        job.setMapperClass(SecondaryMapper.class);
        job.setReducerClass(SecondaryReducer.class);

        //设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\bingh\\OneDrive\\Desktop\\output_sentence\\part-r-00000"));
        FileOutputFormat.setOutputPath(job,new Path("C:\\Users\\bingh\\OneDrive\\Desktop\\output_secondary_sentence"));

        //提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0:1);
    }
}
