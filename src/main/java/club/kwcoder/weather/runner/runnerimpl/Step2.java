package club.kwcoder.weather.runner.runnerimpl;

import club.kwcoder.weather.WeatherStarter;
import club.kwcoder.weather.runner.Runner;
import club.kwcoder.weather.writable.WeatherWritable;
import club.kwcoder.weather.util.HadoopUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Step2 implements Runner {

    @Override
    public void run(WeatherStarter.RunnerBuilder builder) {

    }

    public static void main(String[] args) {

        try {
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询的日期：(dd/MM/yyyy)");
            String date = scanner.next();
            scanner.close();

            Configuration conf = new Configuration();
            conf.set("date", date);
            FileSystem hdfs = FileSystem.get(conf);

            Path input = new Path("/weather_result1");
            Path output = new Path("/weather_result2");
            HadoopUtils.deleteIfExist(output);

            Job job = Job.getInstance(conf, "weather_step1");
            // 设置执行类
            job.setJarByClass(Step2.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置Mapper
            job.setMapperClass(Step2Mapper.class);
            job.setMapOutputKeyClass(WeatherWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 设置Reducer
            job.setReducerClass(Step2Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
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

    private static class Step2Mapper extends Mapper<LongWritable, Text, WeatherWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            String[] items = line.split("\t", 6);
            if (items.length != 6) {
                return;
            }
            // 过滤指定日期
            String date = context.getConfiguration().get("date");
            if (!items[1].equals(date)) {
                return;
            }
            // 构建实体对象
            WeatherWritable weatherWritable = new WeatherWritable.Builder()
                    .setCode(items[0])
                    .setDate(items[1])
                    .setPrecipitation(Float.parseFloat(items[2]))
                    .setMaxTemperature(Float.parseFloat(items[3]))
                    .setMinTemperature(Float.parseFloat(items[4]))
                    .setAvgTemperature(Float.parseFloat(items[5]))
                    .build();
            context.write(weatherWritable, NullWritable.get());
        }
    }


    private static class Step2Reducer extends Reducer<WeatherWritable, NullWritable, WeatherWritable, NullWritable> {
        @Override
        protected void reduce(WeatherWritable key, Iterable<NullWritable> values, Reducer<WeatherWritable, NullWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

}
