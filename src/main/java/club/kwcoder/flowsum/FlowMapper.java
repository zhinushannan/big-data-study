package club.kwcoder.flowsum;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowWritable, Text> {

    private final FlowWritable outKey = new FlowWritable();
    private final Text outVal = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, FlowWritable, Text>.Context context) throws IOException, InterruptedException {
        String valueStr = value.toString();
        if (StringUtils.isBlank(valueStr)) {
            return;
        }

        String[] records = valueStr.split("\t");
        String phone = records[1];
        int upFlow = Integer.parseInt(StringUtils.isBlank(records[4]) ? "0" : records[4]);
        int downFlow = Integer.parseInt(StringUtils.isBlank(records[5]) ? "0" : records[5]);
        int sumFlow = upFlow + downFlow;

        outVal.set(phone);

        outKey.setPhone(phone);
        outKey.setUpFlow(upFlow);
        outKey.setDownFlow(downFlow);
        outKey.setSumFlow(sumFlow);

        System.out.println(outVal);
        context.write(outKey, outVal);
    }
}
