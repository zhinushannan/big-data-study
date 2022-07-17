package club.kwcoder.covid;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CovidReducer extends Reducer<CovidWritable, NullWritable, CovidWritable, NullWritable> {



    @Override
    protected void reduce(CovidWritable key, Iterable<NullWritable> values, Reducer<CovidWritable, NullWritable, CovidWritable, NullWritable>.Context context) throws IOException, InterruptedException {

        context.write(key, NullWritable.get());

    }
}
