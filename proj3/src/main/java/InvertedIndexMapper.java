import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String docName = fileSplit.getPath().getName();

        Integer docNameLen = docName.length();
        docName = docName.substring(0,docNameLen - 14);
        
        Text newKey = new Text();
        Text newVal = new Text();
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            newKey.set(itr.nextToken());
            newVal.set(docName + "##xuzhe##1");
            context.write(newKey, newVal);
        }
    }
}
