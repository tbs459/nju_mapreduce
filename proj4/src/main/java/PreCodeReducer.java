import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PreCodeReducer extends Reducer<Text, Text, Text, Text> { 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        
        Map<String,String> temp_map = new HashMap<String,String>();
        for (Text value : values) {
            temp_map.put(key.toString(), value.toString() );
        }

        context.write(key, new Text("+"));
    }
}
