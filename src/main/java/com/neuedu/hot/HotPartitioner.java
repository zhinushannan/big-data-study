package com.neuedu.hot;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HotPartitioner extends Partitioner<HotWritable, NullWritable> {

    @Override
    public int getPartition(HotWritable hotWritable, NullWritable nullWritable, int numPartitions) {
        // 自定义分区算法
        // 1949 - 1940 = 9, 9 % 3 = 0
        // 1950 - 1940 = 10, 10 % 3 = 1
        // 1951 - 1940 = 11, 11 % 3 = 2
        return (hotWritable.getYear() - 1940) % numPartitions;
    }
}
