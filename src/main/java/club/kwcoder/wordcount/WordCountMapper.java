package club.kwcoder.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    /*
    每输入一行数据就会执行一次map方法，就需要使用一次Text对象和IntWritable对象，
    如果反复创建，会导致消耗大量虚拟机资源。
    因此可以将这两个对象作为成员属性，只进行一次初始化，通过set方法修改其中的值。
     */
    private final Text TEXT = new Text();
    private final IntWritable INT = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // Text对象反序列化为String
        String word = value.toString();
        // 清洗数据：判断该行是否为空行
        if (StringUtils.isBlank(word)) {
            return;
        }
        // 分割数据，得到词的StringTokenizer对象
        StringTokenizer words = new StringTokenizer(word);
        // 将词写入context，供Reduce接收
        while (words.hasMoreTokens()) {
            TEXT.set(words.nextToken());
            context.write(TEXT, INT);
        }
    }
}
