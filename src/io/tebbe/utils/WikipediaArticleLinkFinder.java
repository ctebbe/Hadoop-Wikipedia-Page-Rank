package io.tebbe.utils;

import java.util.*;

/**
 * Created by ct.
 */
public class WikipediaArticleLinkFinder {

    // format: [[INTERNAL_TITLE | HYPERLINK_TEXT]]
    public static final char BEGIN_LINK = '[';
    public static final char SPLIT_LINK = '|';
    public static final char END_LINK = ']';

    public static List<String> findOutgoingLinksFromArticleText(String articleText) {

        List<String> linkTitles = new ArrayList<>();
        String txtNoSpace = articleText.replaceAll("\\s+", "");

        boolean newLinkFound = false;
        for(int i=0; i < txtNoSpace.length(); i++) {
            char ch = txtNoSpace.charAt(i);
            if(ch == BEGIN_LINK) {
                newLinkFound = true;
                continue;
            } else if(newLinkFound) {
                int endLinkIndexTitle = txtNoSpace.indexOf(SPLIT_LINK, i); // find end title index
                if(endLinkIndexTitle != -1) { // valid link with separator
                    linkTitles.add(txtNoSpace.substring(i, endLinkIndexTitle)); 
                    i = txtNoSpace.indexOf(END_LINK, i) + 1; // [[...]]i<-- next desirable index
                }
                newLinkFound = false;
            }
        }
        return linkTitles;
    }
}
