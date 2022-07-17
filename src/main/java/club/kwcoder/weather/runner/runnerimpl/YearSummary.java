package club.kwcoder.weather.runner.runnerimpl;

import club.kwcoder.weather.WeatherStarter;
import club.kwcoder.weather.runner.Runner;
import club.kwcoder.weather.util.ValidateUtils;
import club.kwcoder.weather.writable.WeatherWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * 按年汇总数据
 */
public class YearSummary implements Runner {

    @Override
    public void run(WeatherStarter.RunnerBuilder builder) {
        try {
            Job job = Job.getInstance(builder.getConf(), builder.getJobName());
            // 设置执行类
            job.setJarByClass(DataCleaning.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, builder.getInput());
            // 设置Mapper
            job.setMapperClass(YearSummaryMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritableSummary.class);
            // 设置Reducer
            job.setReducerClass(YearSummaryReducer.class);
            job.setOutputKeyClass(WeatherWritableSummary.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, builder.getOutput());
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println(builder.getJobName() + " process success");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private static class YearSummaryMapper extends Mapper<LongWritable, Text, Text, WeatherWritableSummary> {

        private final Text outKey = new Text();
        private final WeatherWritableSummary.Builder outValBuilder = new WeatherWritableSummary.Builder();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritableSummary>.Context context) throws IOException, InterruptedException {
            /*
            编号     日期        降水量  最高温度  最低温度  平均温度
            83377	01/01/1963	0.0	29.0	16.7	21.74
            83377	01/01/1964	3.2	26.0	18.0	20.84
            83377	01/01/1965	21.2	24.7	16.6	19.66
            83377	01/01/1966	20.0	27.8	17.5	20.7
            83377	01/01/1967	0.1	27.6	15.4	21.2
            83377	01/01/1968	0.0	28.6	17.8	22.28
            83377	01/01/1970	45.2	26.3	16.3	19.8
            83377	01/01/1971	0.0	30.3	18.5	24.02
             */
            String line = value.toString();
            String[] items = ValidateUtils.splitAndValidate(line, "\t", 6);
            if (null == items) {
                return;
            }
            WeatherWritableSummary outVal = outValBuilder
                    .setCode(items[0])
                    .setYear(items[1].split("/")[2])
                    .setPrecipitation(Float.parseFloat(items[2]))
                    .setMaxTemperature(Float.parseFloat(items[3]))
                    .setMinTemperature(Float.parseFloat(items[4]))
                    .setRainDays(items[2].equals("0.0") ? 0 : 1)
                    .buildSummary();
            outKey.set(outVal.getCode() + "-" + outVal.getYear());
            context.write(outKey, outVal);
        }
    }

    private static class YearSummaryReducer extends Reducer<Text, WeatherWritableSummary, WeatherWritable, NullWritable> {
        private final WeatherWritableSummary.Builder outKeyBuilder = new WeatherWritableSummary.Builder();
        private final NullWritable outVal = NullWritable.get();
        @Override
        protected void reduce(Text key, Iterable<WeatherWritableSummary> values, Reducer<Text, WeatherWritableSummary, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String code = "", year = "";
            int rainDays = 0;
            float precipitation = 0.0f, maxTemp = Float.MIN_VALUE, minTemp = Float.MAX_VALUE;
            for (WeatherWritableSummary value : values) {
                if (ValidateUtils.validate(code) || ValidateUtils.validate(year)) {
                    code = value.getCode();
                    year = value.getYear();
                }
                precipitation += value.getPrecipitation();
                maxTemp = Math.max(maxTemp, value.getMaxTemperature());
                if (value.getMinTemperature() != 0.0F) {
                    minTemp = Math.min(minTemp, value.getMinTemperature());
                }
                rainDays += value.getRainDays();
            }
            WeatherWritableSummary outKey = outKeyBuilder
                    .setCode(code)
                    .setYear(year)
                    .setPrecipitation(precipitation)
                    .setMaxTemperature(maxTemp)
                    .setMinTemperature(minTemp)
                    .setRainDays(rainDays)
                    .buildSummary();
            // code year precipitation maxTemperature minTemperature rainDays
            context.write(outKey, outVal);
        }
    }

    private static class WeatherWritableSummary extends WeatherWritable {
        private String year;
        private Integer rainDays;

        public WeatherWritableSummary(Builder builder) {
            super(builder);
            this.year = builder.year;
            this.rainDays = builder.rainDays;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.year);
            out.writeInt(this.rainDays);
            out.writeUTF(super.getCode());
            out.writeFloat(super.getPrecipitation());
            out.writeFloat(super.getMaxTemperature());
            out.writeFloat(super.getMinTemperature());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.year = in.readUTF();
            this.rainDays = in.readInt();
            super.setCode(in.readUTF());
            super.setPrecipitation(in.readFloat());
            super.setMaxTemperature(in.readFloat());
            super.setMinTemperature(in.readFloat());
        }

        @Override
        public String toString() {
            return super.getCode() + '\t' + this.getYear() + '\t' + super.getPrecipitation() + '\t' + super.getMaxTemperature() + '\t' + super.getMinTemperature() + "\t" + this.rainDays;
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
                super.setPrecipitation(precipitation);
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
                super.setAvgTemperature(avgTemperature);
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

}
