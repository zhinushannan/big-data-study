package club.kwcoder.flowsum;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowWritable> {

    private final Text outKey = new Text();
    private final FlowWritable outVal = new FlowWritable();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FlowWritable>.Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        if (StringUtils.isBlank(valueStr)) {
            return;
        }

        String[] records = valueStr.split("\t");
        String phone = records[1];
        int upFlow = Integer.parseInt(StringUtils.isBlank(records[4]) ? "0" : records[4]);
        int downFlow = Integer.parseInt(StringUtils.isBlank(records[5]) ? "0" : records[5]);
        int sumFlow = upFlow + downFlow;

        outKey.set(phone);

        outVal.setUpFlow(upFlow);
        outVal.setDownFlow(downFlow);
        outVal.setSumFlow(sumFlow);

        context.write(outKey, outVal);
    }
}
