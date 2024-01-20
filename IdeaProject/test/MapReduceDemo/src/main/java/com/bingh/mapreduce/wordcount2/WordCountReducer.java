package com.bingh.mapreduce.wordcount2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * KEYIN, reduce阶段输入的key的类型：Text
 * VALUEIN, reduce阶段输入的value的类型：IntWritable
 * KEYOUT, reduce阶段输出的key的类型：Text
 * VALUEOUT, reduce阶段输出的value的类型：IntWritable
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

        int sum = 0;

        // atguigu, (1,1)
        // 累加
        for (IntWritable value : values) {
            sum += value.get();
        }

        outValue.set(sum);

        // 写出
        context.write(key, outValue);

    }

}
