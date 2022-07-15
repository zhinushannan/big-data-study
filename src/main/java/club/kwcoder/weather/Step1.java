package club.kwcoder.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.IOException;

/**
 * 第一步：数据清洗，数据验证，数据导入，三行合一
 */
public class Step1 {

    public static void main(String[] args) {

        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            Path input = new Path("/weather/conventional_weather_stations_inmet_brazil_1961_2019.csv");
            Path output = new Path("/weather_result1");

            Job job = Job.getInstance(conf, "weather_step1");
            // 设置执行类
            job.setJarByClass(Step1.class);
            // 设置输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job, input);
            // 设置Mapper
            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置Reducer
            job.setReducerClass(Step1Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job, output);
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println("step process success");
            }

            HadoopUtils.showAllFiles(output);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }



    }


    private static class Step1Mapper extends Mapper<LongWritable, Text, Text, WeatherWritable> {

        /*
         Estacao;Data;Hora;Precipitacao;TempBulboSeco;TempBulboUmido;TempMaxima;TempMinima;UmidadeRelativa;PressaoAtmEstacao;PressaoAtmMar;DirecaoVento;VelocidadeVento;Insolacao;Nebulosidade;Evaporacao Piche;Temp Comp Media;Umidade Relativa Media;Velocidade do Vento Media;
         82024;01/01/1961;0000;;;;32.3;;;;;;;4.4;;;26.56;82.5;3;
         82024;01/01/1961;1200;;26;23.9;;22.9;83;994.2;;5;5;;8;;;;;
         82024;01/01/1961;1800;;32.3;27;;;65;991.6;;5;3;;9;;;;;
         */
        private static final Text outKey = new Text();
        WeatherWritable.Builder builder = new WeatherWritable.Builder();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (StringUtils.isBlank(line)) {
                return;
            }
            // 跳过标题行：key为0指的是每一块的第一个字符，不是整个文件的，不能只使用key判断
            if (key.equals(new LongWritable(0L)) && line.startsWith("Estacao")) {
                return;
            }
            // 不选择83377巴西利亚的数据
            if (!line.contains("83377")) {
                return;
            }
            /*
            拆分数据：如果指使用;，则在没有数据的地方会直接跳过，即";;;;;".split(";").length为0
            使用强制拆分，在满足条件的情况下拆分成19列
             */
            String[] items = line.split(";", 19);
            // 块切分时可能会把一行切在两块中
            if (items.length != 19) {
                return;
            }
            WeatherWritable weatherWritable = builder
                    .setCode(items[0])
                    .setDate(items[1])
                    .setPrecipitation(StringUtils.isBlank(items[3]) ? 0 : Float.parseFloat(items[3]))
                    .setMaxTemperature(StringUtils.isBlank(items[6]) ? 0 : Float.parseFloat(items[6]))
                    .setMinTemperature(StringUtils.isBlank(items[7]) ? 0 : Float.parseFloat(items[7]))
                    .setAvgTemperature(StringUtils.isBlank(items[16]) ? 0 : Float.parseFloat(items[16]))
                    .build();
            // <83377-01/01/1961, weatherWritable>
            outKey.set(weatherWritable.getCode() + "-" + weatherWritable.getDate());
            context.write(outKey, weatherWritable);
        }
    }

    private static class Step1Reducer extends Reducer<Text, WeatherWritable, WeatherWritable, NullWritable> {

        WeatherWritable.Builder builder = new WeatherWritable.Builder();

        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            String code = null, date = null;
            float precipitation = 0.0F, maxTemp = 0.0F, minTemp = 0.0F, avgTemp = 0.0F;

            for (WeatherWritable value : values) {
                code = value.getCode();
                date = value.getDate();
                precipitation += value.getPrecipitation();
                maxTemp = maxTemp + value.getMaxTemperature();
                minTemp = minTemp + value.getMinTemperature();
                avgTemp = avgTemp + value.getAvgTemperature();
            }

            // 数据验证
            if (avgTemp > maxTemp || avgTemp < minTemp) {
                return;
            }

            WeatherWritable weatherWritable = builder
                    .setCode(code)
                    .setDate(date)
                    .setMaxTemperature(maxTemp)
                    .setMinTemperature(minTemp)
                    .setAvgTemperature(avgTemp)
                    .setPrecipitation(precipitation).build();
            context.write(weatherWritable, NullWritable.get());
        }
    }


}
