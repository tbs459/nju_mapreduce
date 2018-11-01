import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String,Integer> doc_map = new HashMap<String,Integer>();
            for (Text value : values) {
                String[] valuePair = value.toString().split("##xuzhe##"); 
                String docName = valuePair[0];
                Integer docNameCnt = doc_map.get(docName);
                if( docNameCnt == null ){
                    docNameCnt = Integer.parseInt(valuePair[1]);
                }

                doc_map.put(docName, docNameCnt + 1);
            }

            Text newKey = new Text();
            Text newVal = new Text();
            for (Map.Entry<String, Integer> entry : doc_map.entrySet()) {
                newKey.set(key);
                newVal.set(entry.getKey() + "##xuzhe##" + entry.getValue().toString());
                
                context.write(newKey, newVal);
            }

            
            // int splitIndex = key.toString().indexOf(":");

            // //重新设置value值由filename和词频组成
            // info.set( key.toString().substring( splitIndex + 1) +":"+wordcnt );

            // //重新设置key值为单词
            // key.set( key.toString().substring(0,splitIndex));

            // context.write(key, info);
        }
    }
