package club.kwcoder.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class FlowRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        Path input = new Path("/flow/flow.log");
        Path output = new Path("/flow_result");

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(conf, "flowSum");
        // 配置运行类
        job.setJarByClass(FlowRunner.class);
        // 配置输入
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        // 配置Mapper
        job.setMapperClass(FlowMapper.class);
        job.setMapOutputKeyClass(FlowWritable.class);
        job.setMapOutputValueClass(Text.class);
        // 配置Reducer
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(FlowWritable.class);
        job.setOutputValueClass(NullWritable.class);
        // 配置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        // 运行
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("flow sum success");
        }

        hdfs.close();

    }

}
