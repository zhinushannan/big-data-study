package club.kwcoder.wordcount2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountSortASC extends WritableComparator {

    public WordCountSortASC() {
        super(WordCountWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -((WordCountWritable) a).getWord().compareTo(((WordCountWritable) b).getWord());
    }
}
