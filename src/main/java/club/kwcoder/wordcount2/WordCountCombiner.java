package club.kwcoder.wordcount2;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombiner extends Reducer<WordCountWritable, WordCountWritable, WordCountWritable, WordCountWritable> {

    private final WordCountWritable w = new WordCountWritable();

    @Override
    protected void reduce(WordCountWritable key, Iterable<WordCountWritable> values, Reducer<WordCountWritable, WordCountWritable, WordCountWritable, WordCountWritable>.Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (WordCountWritable v : values) {
            sum += v.getCount();
        }
        w.setWord(key.getWord());
        w.setCount(sum);
        context.write(w, w);

    }
}
