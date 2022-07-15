package club.kwcoder.flowsum;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<FlowWritable, Text, FlowWritable, NullWritable> {

    private final FlowWritable outVal = new FlowWritable();

    @Override
    protected void reduce(FlowWritable key, Iterable<Text> values, Reducer<FlowWritable, Text, FlowWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
