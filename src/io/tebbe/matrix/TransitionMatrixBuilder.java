package io.tebbe.matrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class TransitionMatrixBuilder {

    public static class Map extends Mapper<LongWritable, Text, Text, MatrixEditWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strRecord = value.toString();
            String[] indexAndLinks = strRecord.substring(strRecord.indexOf('\t')).split(";");

            int index = Integer.parseInt(indexAndLinks[0]);
            String[] allLinks = indexAndLinks[1].split(",");
            for(String link : allLinks) {
                context.write(new Text(link), new MatrixEditWritable(index, 1/allLinks.length));
            }
        }
    }

    public static class Reduce extends Reducer<Text, MatrixEditWritable, Text, Text> {
        public void reduce(Text key, Iterable<MatrixEditWritable> values, Context context) throws IOException, InterruptedException {
            int index=0;
            for(MatrixEditWritable edits : values) {
                //context.write(key, edits);
            }
        }
    }
}
