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

            Job job1 = new Job(conf, "SocialNetworkCountFirst");
            job1.setNumReduceTasks(5);
            job1.setJarByClass(SocialNetworkCount.class);
            job1.setMapperClass(PreCodeMapper.class);
            job1.setReducerClass(PreCodeReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path("step1"));

            job1.waitForCompletion(true);

            Job job2 = new Job(conf, "SocialNetworkCountSecond");
            job2.setNumReduceTasks(5);
            job2.setJarByClass(SocialNetworkCount.class);
            job2.setMapperClass(MidCodeMapper.class);
            job2.setReducerClass(MidCodeReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path("step1"));
            FileOutputFormat.setOutputPath(job2, new Path("step2"));

            job2.waitForCompletion(job1.isComplete());
            
            Job job3 = new Job(conf, "SocialNetworkCountThird");
            job3.setJarByClass(SocialNetworkCount.class);
            job3.setMapperClass(PostCodeMapper.class);
            job3.setReducerClass(PostCodeReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job3, new Path("step2"));
            FileOutputFormat.setOutputPath(job3, new Path(args[1]));

            job3.waitForCompletion(job2.isComplete());
        } catch (Exception e) { 
            e.printStackTrace(); 
        }
    }
}
