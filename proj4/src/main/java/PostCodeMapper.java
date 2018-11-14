
//import java.io.IOException;
//import java.util.StringTokenizer;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
public static class PostCodeMapper extends Mapper<LongWritable, Text, Text, Text>
{
    private Text newKey = new Text();
    private Text newValue = new Text();
    public void map(LongWritable key,Text value,Context context)
            throws IOException,InterruptedException
    {
        StringTokenizer itr=new StringTokenizer(value.toString());
        newKey.set(itr.nextToken().toString());
        newValue.set(itr.nextToken().toString());
        context.write(newKey, newValue);
    }
}