package io.tebbe.xml;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;

/*
    parses a raw XML Wikipedia article dump page and extracts the title and article text
 */
public class WikipediaXMLDataParser {

    public static final String TAG_TITLE = "title";
    public static final String TAG_TEXT = "text";

    private String articleTitle;
    private String articleText;

    private final File fileToParse;

    public WikipediaXMLDataParser(File file) {
        this.fileToParse = file;
        parse();
    }

    DefaultHandler wikiHandler = new DefaultHandler() {

        StringBuilder accumulatedArticleText = new StringBuilder();

        boolean encounteredTitleTag = false;
        boolean inTextTag = false;

        @Override public void startElement(String uri, String localName, String tagName, Attributes attributes) throws SAXException {
            if(tagName.equalsIgnoreCase(TAG_TITLE)) {
                encounteredTitleTag = true;

            } else if(tagName.equalsIgnoreCase(TAG_TEXT)) {
                inTextTag = true;
            }
        }

        @Override public void endElement(String uri, String localName, String tagName) throws SAXException {
            if(tagName.equalsIgnoreCase(TAG_TEXT)) {
                articleText = accumulatedArticleText.toString();
                inTextTag = false;
            }
        }

        @Override public void characters(char ch[], int start, int length) throws SAXException {
            if(encounteredTitleTag) {
                articleTitle = new String(ch, start, length);
                encounteredTitleTag = false;
            } else if(inTextTag) {
                accumulatedArticleText.append(new String(ch, start, length));
            }
        }
    };

    private void parse() {
        try {
            SAXParserFactory factory = SAXParserFactory.newInstance();
            SAXParser parser = factory.newSAXParser();
            parser.parse(fileToParse, wikiHandler);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getArticleTitle() {
        return articleTitle;
    }

    public String getArticleText() {
        return articleText;
    }

    public static void main(String[] args) {
        WikipediaXMLDataParser p = new WikipediaXMLDataParser(new File("./example/example_data"));
        System.out.println(p.articleTitle);
        System.out.println(p.articleText);
    }
}
