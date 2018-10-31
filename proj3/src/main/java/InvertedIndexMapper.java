import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String docName = fileSplit.getPath().getName();
        String count = "1";
        Text newKey = new Text();
        Text newVal = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String combinedValue = itr.nextToken() + ":" + docName;
            newKey.set(combinedValue);
            newVal.set(count);
            context.write(newKey, newVal);
        }
    }
}
