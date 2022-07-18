package club.kwcoder.weather.runner;

import club.kwcoder.weather.writable.ForecastWritable;
import club.kwcoder.weather.writable.WeatherWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Forecast {

    public static class ForecastMapper extends Mapper<LongWritable, WeatherWritable, Text, WeatherWritable> {
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, WeatherWritable value, Mapper<LongWritable, WeatherWritable, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            String[] split = value.getDate().split("/");
            String date = split[0] + "/" + split[1];
            outKey.set(date);
            context.write(outKey, value);
        }
    }

    public static class ForecastReducer extends Reducer<Text, WeatherWritable, ForecastWritable, NullWritable> {

        private final ForecastWritable.Builder outKeyBuilder = new ForecastWritable.Builder();
        private final NullWritable outVal = NullWritable.get();

        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, ForecastWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            float sumMaxTemp = 0.0F, sumMinTemp = 0.0F, sumAvgTemp = 0.0F, sumPrecipitation = 0.0F;
            int validMaxTemp = 0, validMinTemp = 0, validAvgTemp = 0, validPrecipitation = 0;
            for (WeatherWritable value : values) {
                if (value.getMaxTemperature() != 0.0F) {
                    sumMaxTemp += value.getMaxTemperature();
                    validMaxTemp++;
                }
                if (value.getMinTemperature() != 0.0F) {
                    sumMinTemp += value.getMinTemperature();
                    validMinTemp++;
                }
                if (value.getAvgTemperature() != 0.0F) {
                    sumAvgTemp += value.getAvgTemperature();
                    validAvgTemp++;
                }
                if (value.getPrecipitation() != 0.0F) {
                    sumPrecipitation += value.getPrecipitation();
                    validPrecipitation++;
                }
            }
            ForecastWritable outKey = outKeyBuilder
                    .setDate(key.toString())
                    .setMaxTemperature(sumMaxTemp / validMaxTemp)
                    .setMinTemperature(sumMinTemp / validMinTemp)
                    .setAvgTemperature(sumAvgTemp / validAvgTemp)
                    .setPrecipitation(validPrecipitation == 0 ? 0.0F : sumPrecipitation / validPrecipitation)
                    .build();
            System.out.println(outKey);
            context.write(outKey, outVal);
        }
    }

}
