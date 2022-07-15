package com.neuedu.wordcount2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {



    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            if (args.length != 2) {
                // throw new RuntimeException("need src and dst args");
                args = new String[]{"/books", "/wc_res1"};
            }
            // 输入/输出目录
            Path src = new Path(args[0]);
            Path dst = new Path(args[1]);

            if (hdfs.exists(dst)) {
                hdfs.delete(dst, true);
            }

            // 构建Job任务
            Job job = Job.getInstance(conf, "wordcount");
            // 设置运行类
            job.setJarByClass(WordCount.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, src);
            // 设置Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            // 设置Reduce
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, dst);

            boolean flag = job.waitForCompletion(true);

            if (flag) {
                System.out.println("process success");
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }



    private static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static final Text word = new Text();
        private static final LongWritable count = new LongWritable();


        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (StringUtils.isBlank(line)) {
                return;
            }

            StringTokenizer stringTokenizer = new StringTokenizer(line);
            while (stringTokenizer.hasMoreTokens()) {
                word.set(stringTokenizer.nextToken());
                count.set(1L);
                context.write(word, count);
            }

        }
    }

    private static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private static final LongWritable count = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            count.set(sum);
            context.write(key, count);
        }
    }


}
