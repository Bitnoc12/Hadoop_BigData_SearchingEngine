package WordOffset;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class WordOffsetReducer extends Reducer<Text, LongWritable, Text, Text> {
    private Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        StringBuilder offsets = new StringBuilder();

        for (LongWritable offset : values) {
            offsets.append(offset.toString());
        }

        result.set(offsets.toString());
        context.write(key, result);
    }
}