package club.kwcoder.weather;

import club.kwcoder.weather.runner.Forecast;
import club.kwcoder.weather.runner.YearSummary;
import club.kwcoder.weather.util.HadoopUtils;
import club.kwcoder.weather.writable.ForecastWritable;
import club.kwcoder.weather.writable.WeatherWritable;
import club.kwcoder.weather.writable.WeatherWritableSummary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * 启动类
 */
public class WeatherStarter {

    /**
     * 运行时参数，用于添加输入/输出路径
     */
    private static Map<String, String> TABLE;
    private static Map<String, String[]> FIELD;
    private static Map<String, Class<? extends DBWritable>> CLASS;

    public static final String MysqlDriverClass = "com.mysql.cj.jdbc.Driver";
    public static final String MysqlUrl = "jdbc:mysql://localhost:3306/weather";
    public static final String username = "root";
    public static final String password = "09140727";

    static {
        TABLE = new HashMap<>();
        FIELD = new HashMap<>();
        CLASS = new HashMap<>();

        TABLE.put("data_cleaning_output_table", WeatherWritable.tableName);
        FIELD.put("data_cleaning_output_field", WeatherWritable.fields);
        CLASS.put("data_cleaning_output_class", WeatherWritable.class);

        TABLE.put("year_summary_input_table", TABLE.get("data_cleaning_output_table"));
        FIELD.put("year_summary_input_field", FIELD.get("data_cleaning_output_field"));
        CLASS.put("year_summary_input_class", CLASS.get("data_cleaning_output_class"));

        TABLE.put("year_summary_output_table", WeatherWritableSummary.tableName);
        FIELD.put("year_summary_output_field", WeatherWritableSummary.fields);
        CLASS.put("year_summary_output_class", WeatherWritableSummary.class);


        TABLE.put("forecast_input_table", TABLE.get("data_cleaning_output_table"));
        FIELD.put("forecast_input_field", FIELD.get("data_cleaning_output_field"));
        CLASS.put("forecast_input_class", CLASS.get("data_cleaning_output_class"));

        TABLE.put("forecast_output_table", ForecastWritable.tableName);
        FIELD.put("forecast_output_field", ForecastWritable.fields);
        CLASS.put("forecast_output_class", ForecastWritable.class);

    }

    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, InstantiationException {
//        DataCleaning.runner();
//        run("year_summary", YearSummary.YearSummaryMapper.class, YearSummary.YearSummaryReducer.class);
        run("forecast", Forecast.ForecastMapper.class, Forecast.ForecastReducer.class, WeatherWritable.class, ForecastWritable.class);
    }

    /**
     * 根据Runner接口的实现类，利用反射机制获取该类实例和run方法并调用
     */
    public static void run(String jobName,
                           Class<? extends Mapper<LongWritable, ? extends DBWritable, Text, ? extends DBWritable>> mapper,
                           Class<? extends Reducer<Text, ? extends DBWritable, ? extends DBWritable, NullWritable>> reducer,
                           Class<? extends DBWritable> mapOV, Class<? extends DBWritable> redOK) {
        Configuration conf = HadoopUtils.getConf();

        DBConfiguration.configureDB(
                conf,
                MysqlDriverClass,
                MysqlUrl,
                username,
                password
        );

        Connection connection = null;
        Statement statement = null;
        try {
            Class.forName(MysqlDriverClass);
            connection = DriverManager.getConnection(MysqlUrl, username, password);
            statement = connection.createStatement();
            String sql = "delete from " + TABLE.get(jobName + "_output_table") + " where 1=1";
            statement.execute(sql);
            connection.commit();
        } catch (Exception ignore) {
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ignore) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }

        try {
            Job job = Job.getInstance(conf, jobName);
            // 设置输入
            job.setInputFormatClass(DBInputFormat.class);
            DBInputFormat.setInput(
                    job,
                    CLASS.get(jobName + "_input_class"),
                    TABLE.get(jobName + "_input_table"),
                    null,
                    null,
                    FIELD.get(jobName + "_input_field")
            );
            // 设置Mapper
            job.setMapperClass(mapper);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(mapOV);
            // 设置Reducer
            job.setReducerClass(reducer);
            job.setOutputKeyClass(redOK);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出
            job.setOutputFormatClass(DBOutputFormat.class);
            DBOutputFormat.setOutput(
                    job,
                    TABLE.get(jobName + "_output_table"),
                    FIELD.get(jobName + "_output_field")
            );
            // 运行
            boolean flag = job.waitForCompletion(true);
            if (flag) {
                System.out.println(jobName + " process success");
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }


    }

}
