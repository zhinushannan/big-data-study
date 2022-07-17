package club.kwcoder.covid;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CovidCombinerGrouping extends WritableComparator {

    public CovidCombinerGrouping() {
        super(CovidWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 当属于同一县时，归位一组
        return ((CovidWritable) a).getCounty().compareTo(((CovidWritable) b).getCounty());
    }
}