package com.neuedu.hdfs_demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Demo4 {

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();

        SequenceFile.Writer writer = SequenceFile.createWriter(
                conf,
                SequenceFile.Writer.file(new Path("/seq/books.seq")),
                SequenceFile.Writer.keyClass(Text.class),
                SequenceFile.Writer.valueClass(Text.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK)
                );

        writer.append(new Text("test"), new Text("test"));

        writer.close();

    }

}
