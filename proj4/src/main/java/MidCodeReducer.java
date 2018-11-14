import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MidCodeReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> listOfConnections = new ArrayList<>();
        Text newKey = new Text();
        Text newVal = new Text();
        newVal.set("+");
        values.forEach((value) -> {
           listOfConnections.add(value.toString());
           newKey.set(key.toString()+"+"+value.toString());
            try {
                context.write(newKey, newVal);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        for(int i=0; i<listOfConnections.size(); i++) {
            for (int j = i + 1; j < listOfConnections.size(); j++) {
                String initial = listOfConnections.get(i).trim();
                String second = listOfConnections.get(j).trim();
                if (Integer.parseInt(initial) < Integer.parseInt(second)) {
                    newKey.set(initial + "+" + second);
                    newVal.set("-");
                }
                else {
                    newKey.set(second + "+" + initial);
                    newVal.set("-");
                }
                context.write(newKey, newVal);
            }
        }
    }
}