package com.hadoop.HDFS;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.FileInputStream;
import org.apache.hadoop.io.IOUtils;
import java.net.URI;
import java.io.InputStream;

public class HDFSTest {
    public static void main(String[] args) throws Exception {


        String uri = "hdfs://master:9000/input/LICENSE.txt";
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(uri),
                conf);
        InputStream fileInputStream = null;
        try {
            fileInputStream = fileSystem.open(new Path(uri));
            IOUtils.copyBytes(fileInputStream, System.out, 4096,
                    false);
        } finally {
            IOUtils.closeStream(fileInputStream);
        }
    }

}
