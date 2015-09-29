package io.tebbe.preprocess;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class MatrixIndexMapper extends Mapper<Text, Text, Text, Text> {

    @Override protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String sKey = key.toString();
        if(sKey.equals("!_all-links_")) {
            int index=0;
            for(String wikiKey : value.toString().split(","))
                context.write(new Text(wikiKey), new Text("index:" + String.valueOf(index++)));
        } else
            context.write(key, new Text("links:" + value));
    }
}
