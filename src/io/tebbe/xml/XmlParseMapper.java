package io.tebbe.xml;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class XmlParseMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Context context;

    @Override public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.context = context;
        context.write(new Text(key.toString()), new Text(value));
    }
}
