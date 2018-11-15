//import java.io.IOException;
//import java.util.ArrayList;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;
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
public class PostCodeReducer extends Reducer<Text, Text, Text, Text>
{
    private static int result = 0;
    public void cleanup(Context context) throws IOException, InterruptedException
    {
        context.write(new Text("Result: "), new Text(""+result));
    }
    public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
    {
        int cnt = 0;
        boolean flag = false;
        for(Text value : values)
        {
            if(value.toString().equalsIgnoreCase("+"))
                flag = true;
            else if(value.toString().equalsIgnoreCase("-"))
                cnt ++;
        }
        if (flag) result += cnt;
    }
}