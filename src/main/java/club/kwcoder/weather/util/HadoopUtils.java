package club.kwcoder.weather.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Hadoop 工具类
 * @author zhinushannan
 */
public class HadoopUtils {

    /**
     * hadoop配置
     */
    private static final Configuration conf;

    /**
     * hdfs文件系统
     */
    private static final FileSystem hdfs;

    static {
        conf = new Configuration();
        try {
            hdfs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 若指定目标路径存在，则删除
     * @param targetPath 指定目标路径
     */
    public static void deleteIfExist(Path targetPath) {
        try {
            if (hdfs.exists(targetPath)) {
                hdfs.delete(targetPath, true);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 打印指定路径下的文件内容
     * @param path 指定路径
     */
    public static void showAllFiles(Path path) {

        FSDataInputStream open = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        try {
            if (!hdfs.exists(path)) {
                throw new RuntimeException("target path is not exist!");
            }

            FileStatus[] fileStatuses = hdfs.listStatus(path);

            for (FileStatus fileStatus : fileStatuses) {
                if (fileStatus.isFile()) {
                    open = hdfs.open(fileStatus.getPath());
                    inputStreamReader = new InputStreamReader(open);
                    bufferedReader = new BufferedReader(inputStreamReader);
                    bufferedReader.lines().forEach(System.out::println);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                try {
                    bufferedReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (inputStreamReader != null) {
                try {
                    inputStreamReader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (open != null) {
                try {
                    open.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Configuration getConf() {
        return conf;
    }

    public static FileSystem getHdfs() {
        return hdfs;
    }
}
