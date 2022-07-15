package com.neuedu.hot;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HotGrouping extends WritableComparator {

    public HotGrouping() {
        super(HotWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return Integer.compare(((HotWritable) a).getYear(), ((HotWritable) b).getYear());
    }
}
