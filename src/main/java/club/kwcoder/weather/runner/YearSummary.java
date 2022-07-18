package club.kwcoder.weather.runner;

import club.kwcoder.weather.util.ValidateUtils;
import club.kwcoder.weather.writable.WeatherWritable;
import club.kwcoder.weather.writable.WeatherWritableSummary;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 按年汇总数据，输出结果：观测站编码、年份、年降水量、年最高温度、年最低温度、降水天数
 */
public class YearSummary {

    public static class YearSummaryMapper extends Mapper<LongWritable, WeatherWritable, Text, WeatherWritableSummary> {

        private final Text outKey = new Text();
        private final WeatherWritableSummary.Builder outValBuilder = new WeatherWritableSummary.Builder();

        @Override
        protected void map(LongWritable key, WeatherWritable value, Mapper<LongWritable, WeatherWritable, Text, WeatherWritableSummary>.Context context) throws IOException, InterruptedException {
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
            WeatherWritableSummary outVal = outValBuilder
                    .setCode(value.getCode())
                    .setYear(value.getDate().split("/")[2])
                    .setPrecipitation(value.getPrecipitation())
                    .setMaxTemperature(value.getMaxTemperature())
                    .setMinTemperature(value.getMinTemperature())
                    .setRainDays(value.getPrecipitation() == 0.0F ? 0 : 1)
                    .setAvgTemperature(value.getAvgTemperature())
                    .buildSummary();
            outKey.set(outVal.getCode() + "-" + outVal.getYear());
            context.write(outKey, outVal);
        }

    }

    public static class YearSummaryReducer extends Reducer<Text, WeatherWritableSummary, WeatherWritable, NullWritable> {
        private final WeatherWritableSummary.Builder outKeyBuilder = new WeatherWritableSummary.Builder();
        private final NullWritable outVal = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<WeatherWritableSummary> values, Reducer<Text, WeatherWritableSummary, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String code = "", year = "";
            int rainDays = 0;
            float precipitation = 0.0F, maxTemp = Float.MIN_VALUE, minTemp = Float.MAX_VALUE, avgTemp = 0.0F;
            int avgTempNum = 0;
            // 计算年降雨量、年最高气温、年最低气温、降雨天数
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
                if (value.getAvgTemperature() != 0.0F) {
                    avgTemp += value.getAvgTemperature();
                    avgTempNum++;
                }
            }
            WeatherWritableSummary outKey = outKeyBuilder
                    .setCode(code)
                    .setYear(year)
                    .setPrecipitation(precipitation)
                    .setMaxTemperature(maxTemp)
                    .setMinTemperature(minTemp)
                    .setRainDays(rainDays)
                    .setAvgTemperature(avgTemp / avgTempNum)
                    .buildSummary();
            // 输出格式：code year precipitation maxTemperature minTemperature rainDays
            context.write(outKey, outVal);
        }
    }

}
