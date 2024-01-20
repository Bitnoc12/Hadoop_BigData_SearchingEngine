package WordOffset;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordOffsetDriver {
    public static void main(String[] args) throws Exception {

        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        //为解决空格问题加入
           // conf.set("hadoop.tmp.dir","C:\\tmp");

        // 创建一个新的作业
        Job job = Job.getInstance(conf, "Word Offset Job");

        // 设置主类
        job.setJarByClass(WordOffsetDriver.class);

        // 设置Mapper和Reducer类
        job.setMapperClass(WordOffsetMapper.class);
        job.setReducerClass(WordOffsetReducer.class);

        // 设置Mapper的输出键值类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置Reducer的输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 提交作业并等待完成
        boolean success = job.waitForCompletion(true);

        // 根据作业是否成功完成返回适当的退出码
        System.exit(success ? 0 : 1);
    }
}
