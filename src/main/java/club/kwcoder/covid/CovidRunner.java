package club.kwcoder.covid;

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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

public class CovidRunner {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);

        Path input = new Path("/covid");
        Path output = new Path("/covid_result");

//        Path input = new Path("/Users/zhinushannan/Downloads/us-counties.csv");
//        Path output = new Path("/Users/zhinushannan/Downloads/result");

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        // 配置Job
        Job job = Job.getInstance(conf, "covid");
        job.setJarByClass(CovidRunner.class);
        // 配置输入
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, input);
        // 配置Mapper类
        job.setMapperClass(CovidMapper.class);
        job.setMapOutputKeyClass(CovidWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 设置Combiner阶段的分组
        job.setCombinerKeyGroupingComparatorClass(CovidCombinerGrouping.class);
        // 设置Combiner
        job.setCombinerClass(CovidCombiner.class);
        // 设置自定义排序
        job.setSortComparatorClass(CovidSortByCaseASC.class);
        // 配置Reducer类
        job.setReducerClass(CovidReducer.class);
        job.setOutputKeyClass(CovidWritable.class);
        job.setOutputValueClass(NullWritable.class);
        // 配置输出
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, output);
        // 运行
        boolean flag = job.waitForCompletion(true);
        if (flag) {
            System.out.println("covid process success");
        }

    }

}
