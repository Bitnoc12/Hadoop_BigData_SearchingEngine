package WordOffset;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class WordOffsetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text word = new Text();
    private LongWritable offset = new LongWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length == 2) {
            String wordText = parts[0];
            String info = parts[1];
            word.set(wordText);
            offset.set(Long.parseLong(key.toString()));
            context.write(word, offset);
        }
    }
}
