package com.bingh.mapreduce.secondary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SecondaryReducer extends Reducer<Text, LongWritable,Text,LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
        //因reduce内置的情况会对重复的key进行values的总和
        //这里根据每一个key不可能相同进行每次循环一次
        for (LongWritable value : values) {
            context.write(key, value);
        }
    }
}
