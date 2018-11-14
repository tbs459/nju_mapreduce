import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;



public class MidCodeMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
        Text firstID = new Text();
        Text secondID = new Text();
        while (tokenizer.hasMoreTokens()){
            String[] socialIDs = tokenizer.nextToken().split("\\+");
            firstID.set(socialIDs[0]);
            secondID.set(socialIDs[1]);
            context.write(firstID, secondID);
        }
    }
}
