package io.tebbe.input;

/**
 * Created by ctebbe on 9/28/15.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class XmlDriver
{

    public static class XmlInputFormat1 extends TextInputFormat {

        public static final String START_TAG_KEY = "xmlinput.start";
        public static final String END_TAG_KEY = "xmlinput.end";


        public RecordReader<LongWritable, Text> createRecordReader(
                InputSplit split, TaskAttemptContext context) {
            return new XmlRecordReader();
        }

        /**
         * XMLRecordReader class to read through a given xml document to output
         * xml blocks as records as specified by the start tag and end tag
         *
         */

        public static class XmlRecordReader extends
                RecordReader<LongWritable, Text> {
            private byte[] startTag;
            private byte[] endTag;
            private long start;
            private long end;
            private FSDataInputStream fsin;
            private DataOutputBuffer buffer = new DataOutputBuffer();

            private LongWritable key = new LongWritable();
            private Text value = new Text();
            @Override
            public void initialize(InputSplit split, TaskAttemptContext context) throws InterruptedException, IOException {
                Configuration conf = context.getConfiguration();
                startTag = conf.get(START_TAG_KEY).getBytes("utf-8");
                endTag = conf.get(END_TAG_KEY).getBytes("utf-8");
                FileSplit fileSplit = (FileSplit) split;

                // open the file and seek to the start of the split
                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();
                FileSystem fs = file.getFileSystem(conf);
                fsin = fs.open(fileSplit.getPath());
                fsin.seek(start);

            }
            @Override
            public boolean nextKeyValue() throws IOException,
                    InterruptedException {
                if (fsin.getPos() < end) {
                    if (readUntilMatch(startTag, false)) {
                        try {
                            buffer.write(startTag);
                            if (readUntilMatch(endTag, true)) {
                                key.set(fsin.getPos());
                                value.set(buffer.getData(), 0,
                                        buffer.getLength());
                                return true;
                            }
                        } finally {
                            buffer.reset();
                        }
                    }
                }
                return false;
            }
            @Override
            public LongWritable getCurrentKey() throws IOException,
                    InterruptedException {
                return key;
            }

            @Override
            public Text getCurrentValue() throws IOException,
                    InterruptedException {
                return value;
            }
            @Override
            public void close() throws IOException {
                fsin.close();
            }
            @Override
            public float getProgress() throws IOException {
                return (fsin.getPos() - start) / (float) (end - start);
            }

            private boolean readUntilMatch(byte[] match, boolean withinBlock)
                    throws IOException {
                int i = 0;
                while (true) {
                    int b = fsin.read();
                    // end of file:
                    if (b == -1)
                        return false;
                    // save to buffer:
                    if (withinBlock)
                        buffer.write(b);
                    // check if we're matching:
                    if (b == match[i]) {
                        i++;
                        if (i >= match.length)
                            return true;
                    } else
                        i = 0;
                    // see if we've passed the stop point:
                    if (!withinBlock && i == 0 && fsin.getPos() >= end)
                        return false;
                }
            }
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        Context context;

        @Override protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            this.context = context;
            // parse title and text
            String document = value.toString();
            try {
                XMLStreamReader reader =
                        XMLInputFactory.newInstance().createXMLStreamReader(new
                                ByteArrayInputStream(document.getBytes()));
                String propertyTitle = "";
                String propertyText = "";
                String currentElement = "";
                while (reader.hasNext()) {
                    int code = reader.next();
                    switch (code) {
                        case XMLStreamConstants.START_ELEMENT:
                            currentElement = reader.getLocalName();
                            break;
                        case XMLStreamConstants.CHARACTERS:
                            if (currentElement.equalsIgnoreCase("title")) {
                                propertyTitle += reader.getText();
                            } else if (currentElement.equalsIgnoreCase("text")) {
                                propertyText += reader.getText();
                            }
                            break;
                    }
                }
                reader.close();

                //context.write(new Text(propertyTitle.trim()), new Text(propertyText.trim()));
                parseAndPrintLinks(propertyTitle.trim(), propertyText);
            }
            catch(Exception e) {
                throw new IOException(e);

            }
        }

        private void parseAndPrintLinks(String title, String rawText) throws IOException, InterruptedException {
            if(isNotValidTitle(title)) return;
            Text key = new Text(title.replaceAll("\\s", "_").replaceAll(",", ""));
            for(String link : findInternalWikipediaLinks(rawText))
                context.write(key, new Text(link));
        }

        private boolean isNotValidTitle(String title) {
            if(title.startsWith("Wikipedia:"))
                return true;
            return false;
        }

        private Set<String> findInternalWikipediaLinks(String rawText) {
            Set<String> links = new HashSet<>(); // avoid duplicate internal links
            Pattern wikiLinkPattern = Pattern.compile("\\[.+?\\]");
            Matcher matcher = wikiLinkPattern.matcher(rawText);
            while(matcher.find()) {
                String newLink = matcher.group();
                String parsedLinkTitle = getLinkTitleFromLink(newLink);
                if(parsedLinkTitle == null || parsedLinkTitle.isEmpty())
                    continue;
                links.add(parsedLinkTitle);
            }
            return links;
        }

        private String getLinkTitleFromLink(String rawLink) {
            if(isNotInternalWikiLink(rawLink)) return null;

            int start = rawLink.startsWith("[[") ? 2 : 1;
            int end = rawLink.indexOf(']');

            int pipe = rawLink.indexOf('|');
            if(pipe != -1)
                end = pipe;

            int segment = rawLink.indexOf("#");
            if(segment != -1)
                end = segment;

            String linkName = rawLink.substring(start, end)
                    .replaceAll("\\s", "_")
                    .replaceAll(",", "");

            if(linkName.contains("&amp;"))
                linkName = linkName.replace("&amp;", "&");

            return linkName;
        }

        private boolean isNotInternalWikiLink(String rawLink) {
            int start = rawLink.startsWith("[[") ? 2 : 1;
            if(start+2 > rawLink.length()) // no link
                return true;

            if(rawLink.contains(":") || rawLink.contains(",") || rawLink.contains("&"))
                return true;

            char fChar = rawLink.charAt(start);
            if(fChar == '#') return true;
            if(fChar == ',') return true;
            if(fChar == '.') return true;
            if(fChar == '&') return true;
            if(fChar == '\'') return true;
            if(fChar == '-') return true;
            if(fChar == '{') return true;
            return false;
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value + ",");
            }
            context.write(key, new Text(sb.toString().substring(0, sb.length()-1)));
        }
    }



    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();

        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end", "</page>");
        long secs = 1800000;
        conf.setLong("mapred.task.timeout", secs);

        Job job = Job.getInstance(conf);
        job.setJarByClass(XmlDriver.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(XmlDriver.Map.class);
        job.setReducerClass(XmlDriver.Reduce.class);

        job.setInputFormatClass(XmlInputFormat1.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
