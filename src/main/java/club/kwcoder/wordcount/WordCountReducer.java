package club.kwcoder.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable INT = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // 对相同的键进行
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        // 写出结果
        INT.set(sum);
        context.write(key, INT);
    }

}
