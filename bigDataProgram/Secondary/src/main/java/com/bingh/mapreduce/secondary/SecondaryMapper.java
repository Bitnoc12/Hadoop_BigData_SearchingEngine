package com.bingh.mapreduce.secondary;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondaryMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    Text outK = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        //admin	2 (2,1,<3>), (3,1,<3>)
        //将以上Text类型转换成String类型
        String line = value.toString();

        //通过”\t“来进行切割
        String[] lineList = line.split("\t", 2);

        //得到字符串中第一个字
        String word = lineList[0];

        //输出想要的形式是
        //admin 0
        //word 偏移量(倒排中的地址）
        outK.set(word);

        context.write(outK,key);

    }
}
