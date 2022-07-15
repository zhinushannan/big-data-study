package club.kwcoder.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class WordCountRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 加载配置
        Configuration conf = new Configuration();

        // 获取HDFS对象
        FileSystem hdfs = FileSystem.get(conf);

        // 配置输入输出路径
        Path input = new Path("/wc_src/wc.txt");
        Path output = new Path("/wc_result");

        // 如果输出路径存在，需要删除（当输出路径存在时，程序会报错）
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        // 构建Job任务
        Job job = Job.getInstance(conf, "WordCount");
        // 设置运行类
        job.setJarByClass(WordCountRunner.class);
        // 设置输入
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        // 设置Mapper类及其输出的键值类型
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 设置Reducer类
        job.setReducerClass(WordCountReducer.class);
        // 配置输出的键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 设置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        // 运行
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("word count success");
        }
        hdfs.close();

    }

}
