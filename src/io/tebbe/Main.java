package io.tebbe;

import io.tebbe.input.XmlInputFormat;
import io.tebbe.xml.XmlParseMapper;
import io.tebbe.xml.XmlParseReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by ctebbe.
 */
public class Main {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*
        WikipediaXMLDataParser p = new WikipediaXMLDataParser(new File("./example/example_data"));
        for(String s : WikipediaArticleLinkFinder.findOutgoingLinksFromArticleText(p.getArticleText())) {
            System.out.println();
        }
        */

        Configuration conf = new Configuration();
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        conf.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");

        Job job = Job.getInstance(conf, "wiki links");

        job.setJarByClass(Main.class);
        job.setMapperClass(XmlParseMapper.class);
        job.setReducerClass(XmlParseReducer.class);
        job.setInputFormatClass(XmlInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
