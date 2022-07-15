package com.neuedu.hot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HotRunner {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            // 定义输入目录
            String input = "/hot";
            // 定义输出目录
            String output = "/hot_result";
            Path outputPath = new Path(output);
            // 输出不能存在，否则会有异常
            if (hdfs.exists(outputPath)) {
                hdfs.delete(outputPath, true);
            }
            // 构建Job任务
            Job job = Job.getInstance(conf, "every year hot");
            // 设置运行类
            job.setJarByClass(HotRunner.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置Mapper
            job.setMapperClass(HotMapper.class);
            job.setMapOutputKeyClass(HotWritable.class);
            job.setMapOutputValueClass(NullWritable.class);
            // 设置reduceTask数量和分区类算法
            job.setNumReduceTasks(3);
            job.setPartitionerClass(HotPartitioner.class);
            // 设置自定义排序
            job.setSortComparatorClass(HotSortASC.class);
            // 设置分组
            job.setGroupingComparatorClass(HotGrouping.class);
            // 设置Reducer
            job.setReducerClass(HotReducer.class);
            job.setOutputKeyClass(HotWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, outputPath);
            // 运行
            boolean flag = job.waitForCompletion(true);
            // 提示
            if (flag) {
                System.out.println("每年最高温度统计运行结束");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
