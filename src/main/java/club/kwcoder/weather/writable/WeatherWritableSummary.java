package club.kwcoder.weather.writable;

import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

public class WeatherWritableSummary extends WeatherWritable {
    /**
     * 年份
     */
    private String year;
    /**
     * 降雨天数
     */
    private Integer rainDays;

    public WeatherWritableSummary(WeatherWritableSummary.Builder builder) {
        super(builder);
        this.year = builder.year;
        this.rainDays = builder.rainDays;
    }

    public static final String tableName = "weather_year";
    public static final String[] fields = {"code", "precipitation", "maxTemperature",
            "minTemperature", "avgTemperature", "year", "rainDays"};

    @Override
    public void write(PreparedStatement statement) throws SQLException {
        System.out.println(this);
        statement.setString(1, super.getCode());
        statement.setFloat(2, super.getPrecipitation());
        statement.setFloat(3, super.getMaxTemperature());
        statement.setFloat(4, super.getMinTemperature());
        statement.setFloat(5, super.getAvgTemperature());
        statement.setString(6, this.year);
        statement.setInt(7, this.rainDays);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        super.setCode(resultSet.getString(1));
        super.setPrecipitation(resultSet.getFloat(2));
        super.setMaxTemperature(resultSet.getFloat(3));
        super.setMinTemperature(resultSet.getFloat(4));
        super.setAvgTemperature(resultSet.getFloat(5));
        this.year = resultSet.getString(6);
        this.rainDays = resultSet.getInt(7);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeInt(this.rainDays);
        out.writeUTF(super.getCode());
        out.writeFloat(super.getPrecipitation());
        out.writeFloat(super.getMaxTemperature());
        out.writeFloat(super.getMinTemperature());
        out.writeFloat(super.getAvgTemperature());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.rainDays = in.readInt();
        super.setCode(in.readUTF());
        super.setPrecipitation(in.readFloat());
        super.setMaxTemperature(in.readFloat());
        super.setMinTemperature(in.readFloat());
        super.setAvgTemperature(in.readFloat());
    }

    @Override
    public String toString() {
        return super.getCode() + '\t' + this.getYear() + '\t' + super.getPrecipitation() + '\t' + super.getMaxTemperature() + '\t' + super.getMinTemperature() + "\t" + super.getAvgTemperature() + "\t" + this.rainDays;
    }

    public static class Builder extends WeatherWritable.Builder {
        private String year;
        private Integer rainDays;

        public Builder setRainDays(Integer rainDays) {
            this.rainDays = rainDays;
            return this;
        }

        public Builder setYear(String year) {
            this.year = year;
            return this;
        }

        public Builder setCode(String code) {
            super.setCode(code);
            return this;
        }

        public Builder setPrecipitation(Float precipitation) {
            super.setPrecipitation((float) (Math.round(precipitation * 10) / 10));
            return this;
        }

        public Builder setMaxTemperature(Float maxTemperature) {
            super.setMaxTemperature(maxTemperature);
            return this;
        }

        public Builder setMinTemperature(Float minTemperature) {
            super.setMinTemperature(minTemperature);
            return this;
        }

        public Builder setAvgTemperature(Float avgTemperature) {
            super.setAvgTemperature((float) (Math.round(avgTemperature * 10) / 10));
            return this;
        }

        public WeatherWritableSummary buildSummary() {
            return new WeatherWritableSummary(this);
        }

        public Builder() {
        }

    }

    public WeatherWritableSummary(String code, Float precipitation, Float maxTemperature, Float minTemperature, Float avgTemperature, String year, Integer rainDays) {
        super(code, null, precipitation, maxTemperature, minTemperature, avgTemperature);
        this.year = year;
        this.rainDays = rainDays;
    }

    public WeatherWritableSummary(String year, Integer rainDays) {
        this.year = year;
        this.rainDays = rainDays;
    }

    public WeatherWritableSummary() {
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public Integer getRainDays() {
        return rainDays;
    }

    public void setRainDays(Integer rainDays) {
        this.rainDays = rainDays;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        WeatherWritableSummary that = (WeatherWritableSummary) o;

        if (!Objects.equals(year, that.year)) return false;
        return Objects.equals(rainDays, that.rainDays);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (year != null ? year.hashCode() : 0);
        result = 31 * result + (rainDays != null ? rainDays.hashCode() : 0);
        return result;
    }
}
