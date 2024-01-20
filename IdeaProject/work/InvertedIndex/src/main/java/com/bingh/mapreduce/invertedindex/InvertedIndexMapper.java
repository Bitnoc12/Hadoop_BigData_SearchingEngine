package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Word> {

    private Text outKey = new Text();
    private Word outValue = new Word();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Word>.Context context) throws IOException, InterruptedException {

        // 获取一行内容
        // 0 i am a boy a   // map 1
        // 1 i am a girl    // map 2
        String line = value.toString();

        // 分割行内容
        // 文档编号: "0"
        // 文档内容: "i am a boy a"
        String[] linePart = line.split(" ", 2);

        // 转换文档编号的类型，String -> long
        // “0” -> 0
        long docId = Long.parseLong(linePart[0]);

        // 切割文档内容中的各个单词，把切割结果存储到words中
        // "i am a boy a" -> words["i", "am", "a", "boy", "a"]
        String[] words = linePart[1].split(" ");

        int position = 0;

        // 遍历words，封装，写出
        for (String word : words) {

            // 封装
            outKey.set(word);               // 单词
            outValue.setDocId(docId);       // 单词所在的文档
            outValue.setWordPos(position);  // 单词在文档中的位置

            // 写出
            // -------------
            //    map 1
            // "i":    (0,0)
            // "am":   (0,1)
            // "a":    (0,2)
            // "boy":  (0,3)
            // "a":    (0,4)
            // -------------

            // -------------
            //    map 2
            // "i":    (1,0)
            // "am":   (1,1)
            // "a":    (1,2)
            // "girl": (1,3)
            // -------------
            context.write(outKey, outValue);

            position++;

        }

    }

}
