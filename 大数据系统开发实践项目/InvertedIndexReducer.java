package com.bingh.mapreduce.invertedindex;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class InvertedIndexReducer extends TableReducer<Text, LongWritable, NullWritable> {

    private NullWritable outKey = NullWritable.get();
    private Put outValue;

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, NullWritable, Mutation>.Context context) throws IOException, InterruptedException {

        // 1. 创建一个HashSet对象：HashSetId，用于存储该单词出现的所有句子编号
        // 集合中的句子编号都是唯一不重复的（在同一个句子中可能出现多次该单词）
        HashSet<Long> HashSetId = new HashSet<>();

        // 2. 出现该单词的句子编号不重复地添加进集合
        for (LongWritable value : values) {
                HashSetId.add(value.get());
        }

        // 3. 创建一个StringBuilder对象：StringId，用于拼接集合元素的内容
        StringBuilder StringId = new StringBuilder();

        // 3.1 插入 '(' 到StringId
        StringId.append("(");

        // 3.2 遍历集合中的句子编号，追加到StringId
        for (Long sentenceId : HashSetId) {
            StringId.append(sentenceId).append(",");
        }

        // 3.3 以 ')' 替代最后一个逗号
        if (StringId.length() > 0) {
            StringId.setCharAt(StringId.length() - 1, ')');
        }

        // 4. 设置单词为rowKey，创建Put对象
        outValue = new Put(Bytes.toBytes(key.toString()));

        // 5. 指定插入的列族、列名和值
        outValue.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sentenceFrequency"), Bytes.toBytes(String.valueOf(HashSetId.size())));
        outValue.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sentenceId"), Bytes.toBytes(StringId.toString()));

        // 6. 写出
        // reduce输出的key类型为null，写入HBase中reduce的输出key并不重要，重要的是value，value的数据会被写入HBase表
        context.write(outKey, outValue);

    }

}
