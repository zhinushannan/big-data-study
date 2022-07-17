package club.kwcoder.covid;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CovidSortByCaseASC extends WritableComparator {

    public CovidSortByCaseASC() {
        super(CovidWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CovidWritable a1 = (CovidWritable) a;
        CovidWritable b1 = (CovidWritable) b;

        // 当州不同时，根据州名的字典顺序排序
        if (!a1.getState().equals(b1.getState())) {
            return a1.getState().compareTo(b1.getState());
        }

        // 当州相同时，根据确诊数降序排序
        return -Integer.compare(((CovidWritable) a).getCases(), ((CovidWritable) b).getCases());
    }
}
