package club.kwcoder.flowsum;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowWritable implements Writable {

    private Integer upFlow;
    private Integer downFlow;
    private Integer sumFlow;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowWritable that = (FlowWritable) o;

        if (!upFlow.equals(that.upFlow)) return false;
        if (!downFlow.equals(that.downFlow)) return false;
        return sumFlow.equals(that.sumFlow);
    }

    @Override
    public int hashCode() {
        int result = upFlow.hashCode();
        result = 31 * result + downFlow.hashCode();
        result = 31 * result + sumFlow.hashCode();
        return result;
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Integer sumFlow) {
        this.sumFlow = sumFlow;
    }

    public FlowWritable(Integer upFlow, Integer downFlow, Integer sumFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public FlowWritable() {
    }
}
