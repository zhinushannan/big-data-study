package club.kwcoder.weather;

import club.kwcoder.weather.runner.Runner;
import club.kwcoder.weather.runner.runnerimpl.DataCleaning;
import club.kwcoder.weather.runner.runnerimpl.YearSummary;
import club.kwcoder.weather.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.ws.rs.PUT;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class WeatherStarter {

    private static Map<String, String> PATH;

    static {
        PATH = new HashMap<>();
        PATH.put("data_cleaning_input", "/weather");
        PATH.put("data_cleaning_output", "/weather_result/data_cleaning");

        PATH.put("year_summary_input", PATH.get("data_cleaning_output"));
        PATH.put("year_summary_output", "/weather_result/year_summary");


    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
//        run(DataCleaning.class, "DataCleaning", "data_cleaning_input", "data_cleaning_output");
        run(YearSummary.class, "YearSummary", "year_summary_input", "year_summary_output");
    }

    public static void run(Class<? extends Runner> step, String jobName, String inputKey, String outputKey) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, InstantiationException {
        String input = PATH.get(inputKey);
        String output = PATH.get(outputKey);

        Method run = step.getMethod("run", RunnerBuilder.class);
        RunnerBuilder build = new RunnerBuilder()
                .setJobName(jobName)
                .setInput(input)
                .setOutput(output)
                .build();
        Runner runner = step.newInstance();
        run.invoke(runner, build);
    }

    public static class RunnerBuilder {
        private Configuration conf;
        private String jobName;
        private FileSystem hdfs;
        private Path input;
        private Path output;

        private RunnerBuilder() {
        }

        public RunnerBuilder build() {
            this.conf = HadoopUtils.getConf();
            this.hdfs = HadoopUtils.getHdfs();
            if (jobName == null || input == null || output == null) {
                throw new RuntimeException("参数配置不完整！");
            }
            try {
                if (this.hdfs.exists(output)) {
                    hdfs.delete(output, true);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        public RunnerBuilder setJobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public RunnerBuilder setInput(String input) {
            this.input = new Path(input);
            return this;
        }

        public RunnerBuilder setOutput(String output) {
            this.output = new Path(output);
            return this;
        }

        public Configuration getConf() {
            return conf;
        }

        public String getJobName() {
            return jobName;
        }

        public FileSystem getHdfs() {
            return hdfs;
        }

        public Path getInput() {
            return input;
        }

        public Path getOutput() {
            return output;
        }
    }

}
