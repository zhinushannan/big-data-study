package club.kwcoder.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WeatherWritable implements WritableComparable<WeatherWritable> {

    /**
     * 气象站代码
     */
    private String code;
    /**
     * 日期
     */
    private String date;
    /**
     * 降水量
     */
    private Float precipitation;

    /**
     * 最高温度
     */
    private Float maxTemperature;
    /**
     * 最低温度
     */
    private Float minTemperature;
    /**
     * 平均温度
     */
    private Float avgTemperature;

    public static class Builder {
        private String code;
        private String date;
        private Float precipitation;
        private Float maxTemperature;
        private Float minTemperature;
        private Float avgTemperature;

        public Builder setCode(String code) {
            this.code = code;
            return this;
        }

        public Builder setDate(String date) {
            this.date = date;
            return this;
        }

        public Builder setPrecipitation(Float precipitation) {
            this.precipitation = precipitation;
            return this;
        }

        public Builder setMaxTemperature(Float maxTemperature) {
            this.maxTemperature = maxTemperature;
            return this;
        }

        public Builder setMinTemperature(Float minTemperature) {
            this.minTemperature = minTemperature;
            return this;
        }

        public Builder setAvgTemperature(Float avgTemperature) {
            this.avgTemperature = avgTemperature;
            return this;
        }

        public WeatherWritable build() {
            return new WeatherWritable(this);
        }

        public Builder() {}

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.code);
        out.writeUTF(this.date);
        out.writeFloat(this.precipitation);
        out.writeFloat(this.maxTemperature);
        out.writeFloat(this.minTemperature);
        out.writeFloat(this.avgTemperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.code = in.readUTF();
        this.date = in.readUTF();
        this.precipitation = in.readFloat();
        this.maxTemperature = in.readFloat();
        this.minTemperature = in.readFloat();
        this.avgTemperature = in.readFloat();
    }

    @Override
    public int compareTo(WeatherWritable other) {
        if (null == other) {
            return 1;
        }
        // 仅考虑83377巴西利亚的数据
        return this.date.compareTo(other.getDate());
    }

    public WeatherWritable(Builder builder) {
        this.code = builder.code;
        this.date = builder.date;
        this.precipitation = builder.precipitation;
        this.maxTemperature = builder.maxTemperature;
        this.minTemperature = builder.minTemperature;
        this.avgTemperature = builder.avgTemperature;
    }

    public WeatherWritable() {
    }

    @Override
    public String toString() {
        return code + '\t' + date + '\t' + precipitation + '\t' + maxTemperature + '\t' + minTemperature + '\t' + avgTemperature + '\t';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WeatherWritable that = (WeatherWritable) o;

        if (!Objects.equals(code, that.code)) return false;
        if (!Objects.equals(date, that.date)) return false;
        if (!Objects.equals(precipitation, that.precipitation))
            return false;
        if (!Objects.equals(maxTemperature, that.maxTemperature))
            return false;
        if (!Objects.equals(minTemperature, that.minTemperature))
            return false;
        return Objects.equals(avgTemperature, that.avgTemperature);
    }

    @Override
    public int hashCode() {
        int result = code != null ? code.hashCode() : 0;
        result = 31 * result + (date != null ? date.hashCode() : 0);
        result = 31 * result + (precipitation != null ? precipitation.hashCode() : 0);
        result = 31 * result + (maxTemperature != null ? maxTemperature.hashCode() : 0);
        result = 31 * result + (minTemperature != null ? minTemperature.hashCode() : 0);
        result = 31 * result + (avgTemperature != null ? avgTemperature.hashCode() : 0);
        return result;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Float getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(Float precipitation) {
        this.precipitation = precipitation;
    }

    public Float getMaxTemperature() {
        return maxTemperature;
    }

    public void setMaxTemperature(Float maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public Float getMinTemperature() {
        return minTemperature;
    }

    public void setMinTemperature(Float minTemperature) {
        this.minTemperature = minTemperature;
    }

    public Float getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(Float avgTemperature) {
        this.avgTemperature = avgTemperature;
    }

    public WeatherWritable(String code, String date, Float precipitation, Float maxTemperature, Float minTemperature, Float avgTemperature) {
        this.code = code;
        this.date = date;
        this.precipitation = precipitation;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.avgTemperature = avgTemperature;
    }

}
