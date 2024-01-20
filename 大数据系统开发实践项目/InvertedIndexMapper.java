package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private Text outKey = new Text();
    private LongWritable outValue = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        // 1. 获取一行内容
        String line = value.toString();

        // 2. 分割行内容为两个部分
        // 第一部分：句子编号
        // 第二部分：句子内容
        String[] linePart = line.split(" ", 2);

        // 3. 转换句子编号的类型，String -> long
        long sentenceId = Long.parseLong(linePart[0]);

        // 4. 以空格作为分隔符，切割句子内容中的各个单词，把切割结果存储到words中
        String[] words = linePart[1].split(" ");

        // 5. 遍历各个单词，封装，写出
        for (String word : words) {

            // 5.1 封装
            outKey.set(word);
            outValue.set(sentenceId);

            // 5.2 写出
            context.write(outKey, outValue);

        }

    }

}
