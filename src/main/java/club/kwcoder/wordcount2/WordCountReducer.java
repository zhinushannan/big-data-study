package club.kwcoder.wordcount2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<WordCountWritable, WordCountWritable, WordCountWritable, NullWritable> {

    private final WordCountWritable w = new WordCountWritable();

    @Override
    protected void reduce(WordCountWritable key, Iterable<WordCountWritable> values, Reducer<WordCountWritable, WordCountWritable, WordCountWritable, NullWritable>.Context context) throws IOException, InterruptedException {
//        values.forEach(System.out::println);
//        System.out.println("================");

        int sum = 0;
        for (WordCountWritable v : values) {
            sum += v.getCount();
        }
        w.setWord(key.getWord());
        w.setCount(sum);

        // 因为没必要输出键，所以可以输出null的Hadoop序列化对象
        context.write(w, NullWritable.get());
    }

}
