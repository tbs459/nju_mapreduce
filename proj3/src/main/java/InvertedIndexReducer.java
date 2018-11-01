import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> { 
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException { 
        
        Map<String,Integer> doc_map = new HashMap<String,Integer>();
        Integer wordCnt = 0;
        for (Text value : values) {
            String[] valuePair = value.toString().split("##xuzhe##"); 
            String docName = valuePair[0];
            Integer docNameCnt_new = Integer.parseInt(valuePair[1]);
            Integer docNameCnt_ori = doc_map.get(docName);
            if( docNameCnt_ori == null ){
                docNameCnt_ori = 0;
            }
            wordCnt += docNameCnt_new;

            doc_map.put(docName, docNameCnt_ori + docNameCnt_new );
        }
        
        Integer docCnt = doc_map.size();
        Float wordFreq = ((float)wordCnt)/((float)docCnt);
        StringBuilder docName_complex = new StringBuilder();

        docName_complex.append(wordFreq.toString()+",");

        Iterator<Map.Entry<String, Integer>> it_doc_map = doc_map.entrySet().iterator();
        Map.Entry<String, Integer> entry = it_doc_map.next();

        docName_complex.append(entry.getKey()+":"+entry.getValue().toString());

        while(it_doc_map.hasNext()){
            entry = it_doc_map.next();
            docName_complex.append(";" + entry.getKey() + ":" + entry.getValue().toString());
        }

        context.write(key, new Text(docName_complex.toString()));

        // while (it.hasNext()) {
        //     Map.Entry<String, String> entry = it.next();
        //     System.out.println("key= " + entry.getKey() + " and value= " + entry.getValue());
        // }
        // for (Map.Entry<String, Integer> entry : doc_map.entrySet()) {
            
        // }
        
        // int totalcount = 0,singlecount = 0,filecount = 0;
        // Iterator<Text> it = values.iterator();
        // StringBuilder all = new StringBuilder(key.toString() + "\t"); 
        // String docname = "";
        // for(; it.hasNext(); ){ 
        //     if(it.next().toString().equals(docname)){
        //         singlecount++;
        //     } else {
        //         totalcount += singlecount;
        //         filecount++;
        //         docname = it.next().toString();
        //         all.append(docname);
        //         all.append(":" + singlecount);
        //         all.append(";");
        //         singlecount = 1;
        //     }
        // }
        // totalcount += singlecount;
        // all.append(it.next().toString());
        // all.append(":" + singlecount);
        // all.insert(0,(float)totalcount/filecount + ",");
        // context.write(key, new Text(all.toString()));
    }
}
