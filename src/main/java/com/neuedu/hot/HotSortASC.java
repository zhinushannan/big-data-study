package com.neuedu.hot;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HotSortASC extends WritableComparator {

    public HotSortASC() {
        super(HotWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return -Float.compare(((HotWritable) a).getHot(), ((HotWritable) b).getHot());
    }
}
