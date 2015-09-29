package io.tebbe.preprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class MatrixSetupReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Reducer.Context context) throws IOException, InterruptedException {
        String index=null, links=null;
        for(Text value : values) {
            String strValue = value.toString();
            if(strValue.startsWith("index:"))
                index = strValue.substring(strValue.indexOf(':'), strValue.length());
            else if(strValue.startsWith("links:"))
                links = strValue.substring(strValue.indexOf(':'), strValue.length());
        }
        context.write(key, new Text(index + ":" + links));
    }
}
