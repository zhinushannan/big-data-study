package com.neuedu.hot;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HotReducer extends Reducer<HotWritable, NullWritable, HotWritable, NullWritable> {

    @Override
    protected void reduce(HotWritable key, Iterable<NullWritable> values, Reducer<HotWritable, NullWritable, HotWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

}
