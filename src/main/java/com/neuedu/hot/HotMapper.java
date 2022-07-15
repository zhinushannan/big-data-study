package com.neuedu.hot;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HotMapper extends Mapper<LongWritable, Text, HotWritable, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, HotWritable, NullWritable>.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isBlank(line)) {
            return;
        }
        String[] items = line.split("\t");
        if (items.length != 2) {
            return;
        }
        // 取年份、取温度
        String year = items[0].substring(0, 4);
        String hot = items[1];
        // 实例化自定 义实体类
        HotWritable h = new HotWritable(Integer.parseInt(year), Float.parseFloat(hot));
        context.write(h, NullWritable.get());
    }
}
