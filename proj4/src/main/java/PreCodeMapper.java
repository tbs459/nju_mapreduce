import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class PreCodeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        Text newKey = new Text();
        Text newVal = new Text();
        StringTokenizer itr = new StringTokenizer( value.toString(), "\n" );
        while (itr.hasMoreTokens()) {
            String[] pair = itr.nextToken().toString().split(" ");
            String userid1 = pair[0];
            String userid2 = pair[1];
            
            String userKey;
            if ( Integer.parseInt(userid1) > Integer.parseInt(userid2) ){
                userKey = userid2 + "+" + userid1;
            }else{
                userKey = userid1 + "+" + userid2;
            }

            newKey.set(userKey);
            newVal.set("+");
            context.write(newKey, newVal);
        }
    }
}
