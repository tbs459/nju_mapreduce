import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.ByteArrayInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SocialNetworkCount {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "SocialNetworkCount");
            job.setNumReduceTasks(5);
            job.setJarByClass(SocialNetworkCount.class);
            job.setMapperClass(PreCodeMapper.class);
            job.setReducerClass(PreCodeReducer.class); 
            job.setOutputKeyClass(Text.class); 
            job.setOutputValueClass(Text.class); 
            FileInputFormat.addInputPath(job, new Path(args[0])); 
            FileOutputFormat.setOutputPath(job, new Path(args[1])); 
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }
}