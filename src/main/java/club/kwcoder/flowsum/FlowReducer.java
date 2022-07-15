package club.kwcoder.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowWritable, Text, FlowWritable> {

    private final FlowWritable outVal = new FlowWritable();

    @Override
    protected void reduce(Text key, Iterable<FlowWritable> values, Reducer<Text, FlowWritable, Text, FlowWritable>.Context context) throws IOException, InterruptedException {
        int upFlow = 0, downFlow = 0, sumFlow = 0;
        for (FlowWritable value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            sumFlow += value.getSumFlow();
        }

        outVal.setUpFlow(upFlow);
        outVal.setDownFlow(downFlow);
        outVal.setSumFlow(sumFlow);

        context.write(key, outVal);
    }
}
