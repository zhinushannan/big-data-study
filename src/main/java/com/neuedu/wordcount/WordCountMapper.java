package com.neuedu.wordcount;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final Text TEXT = new Text();
    private final IntWritable INT = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String word = value.toString();
        if (StringUtils.isBlank(word)) {
            return;
        }
        StringTokenizer words = new StringTokenizer(word);
        while (words.hasMoreTokens()) {
            TEXT.set(words.nextToken());
            context.write(TEXT, INT);
        }
    }
}
