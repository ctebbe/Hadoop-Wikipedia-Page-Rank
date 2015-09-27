package io.tebbe;

import io.tebbe.xml.WikipediaArticleLinkFinder;
import io.tebbe.xml.WikipediaXMLDataParser;

import java.io.File;

/**
 * Created by ctebbe.
 */
public class Main {
    public static void main(String[] args) {
        WikipediaXMLDataParser p = new WikipediaXMLDataParser(new File("./example/example_data"));
        for(String s : WikipediaArticleLinkFinder.findOutgoingLinksFromArticleText(p.getArticleText())) {
            System.out.println();
        }
    }
}
