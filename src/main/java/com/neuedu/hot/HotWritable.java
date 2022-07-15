package com.neuedu.hot;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class HotWritable implements WritableComparable<HotWritable> {

    private Integer year;
    private Float hot;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeFloat(hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.hot = in.readFloat();
    }

    @Override
    public int compareTo(HotWritable other) {
        // 比null大
        if (other == null) {
            return  1;
        }
        // 年份不同时，只比较年份，升序
        if (!Objects.equals(this.year, other.year)) {
            return Integer.compare(this.year,other.year);
        }
        // 年份相同时，比较温度，升序
        return Float.compare(this.hot, other.hot);
    }

    @Override
    public String toString() {
        return year + "\t" + hot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HotWritable that = (HotWritable) o;

        if (!Objects.equals(year, that.year)) return false;
        return Objects.equals(hot, that.hot);
    }

    @Override
    public int hashCode() {
        int result = year != null ? year.hashCode() : 0;
        result = 31 * result + (hot != null ? hot.hashCode() : 0);
        return result;
    }

    public Integer getYear() {
        return year;
    }

    public void setYear(Integer year) {
        this.year = year;
    }

    public Float getHot() {
        return hot;
    }

    public void setHot(Float hot) {
        this.hot = hot;
    }

    public HotWritable(Integer year, Float hot) {
        this.year = year;
        this.hot = hot;
    }

    public HotWritable() {
    }


}
