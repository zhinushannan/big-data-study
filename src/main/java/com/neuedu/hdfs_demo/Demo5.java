package com.neuedu.hdfs_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

import java.io.IOException;

public class Demo5 {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        FileSystem hdfs = FileSystem.get(conf);

        SequenceFile.Reader reader = new SequenceFile.Reader(
                conf,
                SequenceFile.Reader.file(new Path("/seq/data"))
        );

        Class<? extends Writable> keyClass = (Class<? extends Writable>) reader.getKeyClass();
        Class<? extends Writable> valueClass = (Class<? extends Writable>) reader.getValueClass();

        reader.close();

        MapFile.fix(hdfs, new Path("/seq"), keyClass, valueClass, false, conf);


    }

}
