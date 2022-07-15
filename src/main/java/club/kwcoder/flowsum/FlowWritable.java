package club.kwcoder.flowsum;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowWritable implements WritableComparable<FlowWritable> {

    private String phone;
    private Integer upFlow;
    private Integer downFlow;
    private Integer sumFlow;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.upFlow = in.readInt();
        this.downFlow = in.readInt();
        this.sumFlow = in.readInt();
    }

    @Override
    public String toString() {
        return phone + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    @Override
    public int compareTo(FlowWritable o) {
        return this.phone.compareTo(o.phone);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FlowWritable that = (FlowWritable) o;

        if (phone != null ? !phone.equals(that.phone) : that.phone != null) return false;
        if (upFlow != null ? !upFlow.equals(that.upFlow) : that.upFlow != null) return false;
        if (downFlow != null ? !downFlow.equals(that.downFlow) : that.downFlow != null) return false;
        return sumFlow != null ? sumFlow.equals(that.sumFlow) : that.sumFlow == null;
    }

    @Override
    public int hashCode() {
        int result = phone != null ? phone.hashCode() : 0;
        result = 31 * result + (upFlow != null ? upFlow.hashCode() : 0);
        result = 31 * result + (downFlow != null ? downFlow.hashCode() : 0);
        result = 31 * result + (sumFlow != null ? sumFlow.hashCode() : 0);
        return result;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
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

    public FlowWritable(String phone, Integer upFlow, Integer downFlow, Integer sumFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = sumFlow;
    }

    public FlowWritable() {
    }

}
