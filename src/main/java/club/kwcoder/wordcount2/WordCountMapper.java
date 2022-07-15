package club.kwcoder.wordcount2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, WordCountWritable, WordCountWritable> {

    private final WordCountWritable w = new WordCountWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, WordCountWritable, WordCountWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        StringTokenizer st = new StringTokenizer(line);
        while (st.hasMoreTokens()) {
            String word = st.nextToken();
            w.setWord(word);
            w.setCount(1);
            context.write(w, w);
        }
    }
}
