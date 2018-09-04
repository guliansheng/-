package training;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


public class ReadFileFromLocal {
    public static void main(String[] args) throws Exception {
        String source=args[0];
        String destination=args[1];
        InputStream in = new BufferedInputStream(new FileInputStream(source));
       
        Configuration conf = new Configuration();
        
        //FileSystem fs = FileSystem.get(URI.create(destination),conf);
        FileSystem fs = FileSystem.get(conf);
        OutputStream out = fs.create(new Path(destination));
        IOUtils.copyBytes(in, out, 4096, true);
    }
}