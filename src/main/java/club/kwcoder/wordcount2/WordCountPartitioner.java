package club.kwcoder.wordcount2;

import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartitioner extends Partitioner<WordCountWritable, WordCountWritable> {
    @Override
    public int getPartition(WordCountWritable key, WordCountWritable value, int numPartitions) {
        return Math.abs(key.hashCode()) % 3;
    }
}
