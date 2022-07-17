package club.kwcoder.covid;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CovidMapper extends Mapper<LongWritable, Text, CovidWritable, NullWritable> {

    private final CovidWritable.Builder builder = new CovidWritable.Builder();

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CovidWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 排除空行
        if (StringUtils.isBlank(line)) {
            return;
        }
        // 排除首行
        if (key.get() == 0L && line.startsWith("date")) {
            return;
        }
        // 切分与排除非法数据
        String[] items = line.split(",", 6);
        if (items.length != 6) {
            return;
        }
        for (String item : items) {
            if (StringUtils.isBlank(item)) {
                return;
            }
        }
        // 构建对象并输出
        CovidWritable outKey = builder
                .setState(items[2])
                .setCounty(items[1])
                .setCases(Integer.parseInt(items[4]))
                .setDeaths(Integer.parseInt(items[5])).build();
        context.write(outKey, NullWritable.get());
    }
}
