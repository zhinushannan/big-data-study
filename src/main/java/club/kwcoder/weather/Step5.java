package club.kwcoder.weather;

import club.kwcoder.flowsum.FlowWritable;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Step5 {

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            Path input = new Path("/weather_result3");
            Path output = new Path("/weather_result5");
            HadoopUtils.deleteIfExist(output);

            Job job = Job.getInstance(conf, "weather_step5");
            // 设置执行类
            job.setJarByClass(Step5.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置Mapper
            job.setMapperClass(Step5Mapper.class);
            job.setMapOutputKeyClass(FloatWritable.class);
            job.setMapOutputValueClass(Text.class);
            // 设置排序
            job.setSortComparatorClass(IntDescSort.class);
            // 设置Reducer
            job.setReducerClass(Step5Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, output);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("step2 process success");
            }

            HadoopUtils.showAllFiles(output);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }


    }


    private static class Step5Mapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
        private final FloatWritable outKey = new FloatWritable();
        private final Text outVal = new Text();
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FloatWritable, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            String[] items = line.split("\t", 3);
            if (items.length != 3) {
                return;
            }
            outKey.set(Float.parseFloat(items[2]));
            outVal.set(items[0] + "\t" + items[1]);
            context.write(outKey, outVal);
        }
    }

    private static class Step5Reducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Reducer<FloatWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(value, key);
            }
        }
    }

    private static class IntDescSort extends IntWritable.Comparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }



}
