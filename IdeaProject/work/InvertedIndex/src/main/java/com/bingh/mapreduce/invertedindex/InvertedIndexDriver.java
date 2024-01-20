package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedIndexDriver {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        // 1 获取job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar包路径
        job.setJarByClass(InvertedIndexDriver.class);

        // 3 关联mapper和reducer
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        // 4 设置map输出的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Word.class);

        // 5 设置最终输出的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("C:\\Users\\bingh\\OneDrive\\Desktop\\input2.txt"));
        //FileInputFormat.setInputPaths(job, new Path("D:\\bigData\\sentences.txt\\sentences.txt"));
        FileOutputFormat.setOutputPath(job, new Path("C:\\Users\\bingh\\OneDrive\\Desktop\\output_sentence"));

        // 7 提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);

    }

}
