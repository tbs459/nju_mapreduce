import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> { 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        int totalcount = 0,singlecount = 0,filecount = 0;
        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder(key.toString() + "\t"); 
        String docname = "";
        for(; it.hasNext(); ){ 
            if(it.next().toString().equals(docname))
                singlecount++;
            else
            {
            totalcount += singlecount;
            filecount++;
            docname = it.next().toString();
            all.append(docname);
            all.append(":" + singlecount);
            all.append(";");
            singlecount = 1;
            }
        }
        totalcount += singlecount;
        all.append(it.next().toString());
        all.append(":" + singlecount);
        all.insert(0,(float)totalcount/filecount + ",");
        context.write(key, new Text(all.toString()));
    }
}
