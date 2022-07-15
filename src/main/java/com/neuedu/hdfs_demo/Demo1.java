package com.neuedu.hdfs_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

public class Demo1 {

    public static void main(String[] args) {


        Configuration conf = new Configuration();
        FileSystem hdfs = null;
        try {
            hdfs = FileSystem.get(conf);
//            hdfs.copyFromLocalFile(new Path("/Users/zhinushannan/Downloads/Oxford English Dictionary (Oxford English Dictionary) (z-lib.org).txt"), new Path("/wc_src"));
//            hdfs.copyToLocalFile(new Path("/wc"), new Path("src/main/resources/wc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (hdfs != null) {
                try {
                    hdfs.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
