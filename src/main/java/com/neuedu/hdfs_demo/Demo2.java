package com.neuedu.hdfs_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Demo2 {

    public static void main(String[] args) throws IOException {

        String[] name = {
                "file1.txt",
                "file2.txt",
                "file3.txt",
                "file4.txt",
                "file5.txt",
        };
        String[] data = {
                "One, Two, Three, buckle my shoe",
                "Three Four shut the door",
                "five six pick up sticks",
                "nine ten a big fat hen",
                "sedfrsrsd"
        };

        FileSystem hdfs = null;
        MapFile.Writer writer = null;
        try {
            Configuration conf = new Configuration();
            hdfs = FileSystem.get(conf);
            Path dst = new Path("/books/mybooks");


            writer = new MapFile.Writer(
                    conf,
                    dst,
                    MapFile.Writer.keyClass(Text.class),
                    MapFile.Writer.valueClass(Text.class),
                    MapFile.Writer.compression(SequenceFile.CompressionType.NONE));

            for (int i = 0; i < name.length; i++) {
                writer.append(new Text(name[i]), new Text(data[i]));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
            if (hdfs != null) {
                hdfs.close();
            }
        }


    }

}
