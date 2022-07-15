package club.kwcoder.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class Step3 {

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            Path input = new Path("/weather_result1");
            Path output = new Path("/weather_result3");
            HadoopUtils.deleteIfExist(output);

            Job job = Job.getInstance(conf, "weather_step3");
            // 设置执行类
            job.setJarByClass(Step3.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置Mapper
            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(FloatWritable.class);
            // 设置Reducer
            job.setReducerClass(Step3Reducer.class);
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

    private static class Step3Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private final Text outKey = new Text();
        private final FloatWritable outVal = new FloatWritable();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            // 83377	01/01/1963	0.0	29.0	16.7	21.74
            // 83377	01/01/1964	3.2	26.0	18.0	20.84
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            String[] items = line.split("\t", 6);
            if (items.length != 6) {
                return;
            }
            outKey.set(items[0] + "\t" + items[1].split("/")[2]);
            outVal.set(Float.parseFloat(items[3]));
            context.write(outKey, outVal);
        }
    }

    private static class Step3Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
        private final FloatWritable outVal = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) throws IOException, InterruptedException {
            float maxTemp = Float.MIN_VALUE;
            for (FloatWritable value : values) {
                maxTemp = Math.max(maxTemp, value.get());
            }
            outVal.set(maxTemp);
            context.write(key, outVal);
        }
    }

}
