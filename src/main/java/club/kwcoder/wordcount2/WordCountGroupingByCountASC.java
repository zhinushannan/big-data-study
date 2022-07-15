package club.kwcoder.wordcount2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountGroupingByCountASC extends WritableComparator {

    public WordCountGroupingByCountASC() {
        super(WordCountWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return ((WordCountWritable) a).getCount().compareTo(((WordCountWritable) b).getCount());
    }
}
