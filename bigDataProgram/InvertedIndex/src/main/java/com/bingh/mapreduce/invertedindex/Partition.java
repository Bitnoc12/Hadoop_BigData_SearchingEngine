package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class Partition extends Partitioner<Text, LongWritable> {

    HashMap<String, Integer> partition = new HashMap<>();

    public Partition() {
        setPartition();
    }

    public void setPartition() {

        for (int i = 0; i < 10; i++) {
            String key = String.valueOf(i);
            partition.put(key, i);
        }

        for (int i = 0; i < 26; i++) {
            char letter = (char)('a' + i);
            String key = String.valueOf(letter);
            partition.put(key, i + 10);
        }

    }

    @Override
    public int getPartition(Text text, LongWritable longWritable, int NumPartition) {

        String singleWord = text.toString();

        String preWord = singleWord.substring(0, 1);

        return partition.get(preWord);

    }

}
