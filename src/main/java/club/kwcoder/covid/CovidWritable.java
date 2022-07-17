package club.kwcoder.covid;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CovidWritable implements WritableComparable<CovidWritable> {

    private String state;
    private String county;
    private Integer cases;
    private Integer deaths;

    @Override
    public int compareTo(CovidWritable o) {
        // 若比较对象为null，则自身大
        if (null == o) {
            return 1;
        }
        // 若州不一样，则比较州
        if (!this.state.equals(o.state)) {
            return this.state.compareTo(o.state);
        }
        // 若州一样，则比较县
        return this.county.compareTo(o.county);
        // map()结束时总确诊数还没有计算，因此没必要比较
    }

    /**
     * Hadoop序列化
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(state);
        out.writeUTF(county);
        out.writeInt(cases);
        out.writeInt(deaths);
    }

    /**
     * Hadoop反序列化
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.state = in.readUTF();
        this.county = in.readUTF();
        this.cases = in.readInt();
        this.deaths = in.readInt();
    }

    /**
     * 建造者模式构建对象
     */
    public static class Builder {
        private String state;
        private String county;
        private Integer cases;
        private Integer deaths;

        public Builder setState(String state) {
            this.state = state;
            return this;
        }

        public Builder setCounty(String county) {
            this.county = county;
            return this;
        }

        public Builder setCases(Integer cases) {
            this.cases = cases;
            return this;
        }

        public Builder setDeaths(Integer deaths) {
            this.deaths = deaths;
            return this;
        }

        Builder() {
        }

        CovidWritable build() {
            return new CovidWritable(this);
        }

    }

    public CovidWritable(Builder builder) {
        this.state = builder.state;
        this.county = builder.county;
        this.cases = builder.cases;
        this.deaths = builder.deaths;
    }

    @Override
    public String toString() {
        return state + '\t' + county + '\t' + "\t" + cases + "\t" + deaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CovidWritable that = (CovidWritable) o;

        if (!Objects.equals(state, that.state)) return false;
        if (!Objects.equals(county, that.county)) return false;
        if (!Objects.equals(cases, that.cases)) return false;
        return Objects.equals(deaths, that.deaths);
    }

    @Override
    public int hashCode() {
        int result = state != null ? state.hashCode() : 0;
        result = 31 * result + (county != null ? county.hashCode() : 0);
        result = 31 * result + (cases != null ? cases.hashCode() : 0);
        result = 31 * result + (deaths != null ? deaths.hashCode() : 0);
        return result;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public Integer getCases() {
        return cases;
    }

    public void setCases(Integer cases) {
        this.cases = cases;
    }

    public Integer getDeaths() {
        return deaths;
    }

    public void setDeaths(Integer deaths) {
        this.deaths = deaths;
    }

    public CovidWritable(String state, String county, Integer cases, Integer deaths) {
        this.state = state;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
    }

    public CovidWritable() {
    }
}
