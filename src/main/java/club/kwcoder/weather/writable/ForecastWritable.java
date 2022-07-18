package club.kwcoder.weather.writable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class ForecastWritable implements DBWritable, Writable {

    private String date;
    private Float maxTemperature;
    private Float minTemperature;
    private Float avgTemperature;
    private Float precipitation;

    public ForecastWritable(Builder builder) {
        this.date = builder.date;
        this.maxTemperature = builder.maxTemperature;
        this.minTemperature = builder.minTemperature;
        this.avgTemperature = builder.avgTemperature;
        this.precipitation = builder.precipitation;
    }

    public static class Builder {
        private String date;
        private Float maxTemperature;
        private Float minTemperature;
        private Float avgTemperature;
        private Float precipitation;

        public Builder setDate(String date) {
            this.date = date;
            return this;
        }

        public Builder setMaxTemperature(Float maxTemperature) {
            this.maxTemperature = (float) (Math.round(maxTemperature * 10) / 10);
            return this;
        }

        public Builder setMinTemperature(Float minTemperature) {
            this.minTemperature = (float) (Math.round(minTemperature * 10) / 10);
            return this;
        }

        public Builder setAvgTemperature(Float avgTemperature) {
            this.avgTemperature = (float) (Math.round(avgTemperature * 10) / 10);
            return this;
        }

        public Builder setPrecipitation(Float precipitation) {
            this.precipitation = (float) (Math.round(precipitation * 10) / 10);
            return this;
        }

        public Builder() {
        }

        public ForecastWritable build() {
            return new ForecastWritable(this);
        }

    }


    public static final String tableName = "forecast";
    public static final String[] fields = {"date", "maxTemperature", "minTemperature", "avgTemperature", "precipitation"};

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setString(1, this.date);
        statement.setFloat(2, this.maxTemperature);
        statement.setFloat(3, this.minTemperature);
        statement.setFloat(4, this.avgTemperature);
        statement.setFloat(5, this.precipitation);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.date = resultSet.getString(1);
        this.maxTemperature = resultSet.getFloat(2);
        this.minTemperature = resultSet.getFloat(3);
        this.avgTemperature = resultSet.getFloat(4);
        this.precipitation = resultSet.getFloat(5);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.date);
        out.writeFloat(this.maxTemperature);
        out.writeFloat(this.minTemperature);
        out.writeFloat(this.avgTemperature);
        out.writeFloat(this.precipitation);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.date = in.readUTF();
        this.maxTemperature = in.readFloat();
        this.minTemperature = in.readFloat();
        this.avgTemperature = in.readFloat();
        this.precipitation = in.readFloat();
    }

    @Override
    public String toString() {
        return date + '\t' + maxTemperature + '\t' + minTemperature + '\t' + avgTemperature + '\t' + precipitation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ForecastWritable that = (ForecastWritable) o;

        if (!Objects.equals(date, that.date)) return false;
        if (!Objects.equals(maxTemperature, that.maxTemperature))
            return false;
        if (!Objects.equals(minTemperature, that.minTemperature))
            return false;
        if (!Objects.equals(avgTemperature, that.avgTemperature))
            return false;
        return Objects.equals(precipitation, that.precipitation);
    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (maxTemperature != null ? maxTemperature.hashCode() : 0);
        result = 31 * result + (minTemperature != null ? minTemperature.hashCode() : 0);
        result = 31 * result + (avgTemperature != null ? avgTemperature.hashCode() : 0);
        result = 31 * result + (precipitation != null ? precipitation.hashCode() : 0);
        return result;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
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

    public Float getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(Float precipitation) {
        this.precipitation = precipitation;
    }

    public ForecastWritable(String date, Float maxTemperature, Float minTemperature, Float avgTemperature, Float precipitation) {
        this.date = date;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.avgTemperature = avgTemperature;
        this.precipitation = precipitation;
    }

    public ForecastWritable() {
    }
}
