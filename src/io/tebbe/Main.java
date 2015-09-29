package io.tebbe;

import io.tebbe.input.XmlDriver;
import io.tebbe.matrix.MatrixIndexAssign;
import io.tebbe.matrix.SumKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.text.NumberFormat;

/**
 * Created by ctebbe.
 */
public class Main {

    private static final NumberFormat nf = new DecimalFormat("00");
    public static final String pathLinks = "/output/links";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Main main = new Main();
        if(args.length > 0)
            main.runWikipediaXmlParsing(args[0], args[1]);
        else
            main.runMatrixBuilder("/all_links/part-r-0000", "/result");
    }

    private void runMatrixBuilder(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        long secs = 1800000;
        conf.setLong("mapred.task.timeout", secs);

        String tmp = "/keysum";
        Job sumKeys = Job.getInstance(conf);
        sumKeys.setJarByClass(SumKeys.class);
        sumKeys.setOutputKeyClass(Text.class);
        sumKeys.setOutputValueClass(IntWritable.class);

        sumKeys.setMapperClass(SumKeys.Map.class);
        sumKeys.setInputFormatClass(TextInputFormat.class);

        sumKeys.setReducerClass(SumKeys.Reduce.class);
        sumKeys.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(sumKeys, new Path(inPath));
        FileOutputFormat.setOutputPath(sumKeys, new Path(tmp));
        sumKeys.waitForCompletion(true);

        // get total link size
        Path p = new Path("hdfs://juneau:32401" + tmp + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        int size = Integer.parseInt(br.readLine().split("\t")[1]);

        conf.setInt("size", size);
        Job buildMatrixJob = Job.getInstance(conf);
        buildMatrixJob.setJarByClass(MatrixIndexAssign.class);
        buildMatrixJob.setOutputKeyClass(Text.class);
        buildMatrixJob.setOutputValueClass(Text.class);

        buildMatrixJob.setMapperClass(MatrixIndexAssign.Map.class);
        buildMatrixJob.setInputFormatClass(TextInputFormat.class);

        buildMatrixJob.setReducerClass(MatrixIndexAssign.Reduce.class);
        buildMatrixJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(buildMatrixJob, new Path(inPath));
        FileOutputFormat.setOutputPath(buildMatrixJob, new Path(outPath));

        buildMatrixJob.waitForCompletion(true);
        //System.exit(result ? 0 : 1);
    }

    private void runWikipediaXmlParsing(String inPath, String outPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        long secs = 1800000;
        conf.setLong("mapred.task.timeout", secs);

        Job parseXmlJob = Job.getInstance(conf);
        parseXmlJob.setJarByClass(XmlDriver.class);
        parseXmlJob.setOutputKeyClass(Text.class);
        parseXmlJob.setOutputValueClass(Text.class);

        parseXmlJob.setMapperClass(XmlDriver.Map.class);
        parseXmlJob.setInputFormatClass(XmlDriver.XmlInputFormat1.class);

        parseXmlJob.setReducerClass(XmlDriver.Reduce.class);
        parseXmlJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(parseXmlJob, new Path(inPath));
        FileOutputFormat.setOutputPath(parseXmlJob, new Path(outPath));

        boolean result = parseXmlJob.waitForCompletion(true);
        //System.exit(result ? 0 : 1);
    }
}
