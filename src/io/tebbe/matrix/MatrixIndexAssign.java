package io.tebbe.matrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class MatrixIndexAssign {


    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private final static Text one = new Text("1");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one, value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int index=0;
            int size = context.getConfiguration().getInt("size", -1);
            for(Text value : values) {
                String[] titleAndLinks = value.toString().split("\t");
                context.write(new Text(titleAndLinks[0]), new Text(size + ";" + String.valueOf(index++) + ";" + titleAndLinks[1]));
            }
        }
    }
}
