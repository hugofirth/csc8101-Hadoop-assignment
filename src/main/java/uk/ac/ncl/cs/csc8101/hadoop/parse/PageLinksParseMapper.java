package uk.ac.ncl.cs.csc8101.hadoop.parse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.CharacterCodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PageLinksParseMapper extends Mapper<LongWritable, Text, Text, Text> {

    //The regular expression used to identify link and isolate their contents
    private static final Pattern linksSyntax = Pattern.compile("(?<=[\\[]{2}).+?(?=[\\]])");

    @Override
    public void map(LongWritable key, Text page, Context context) throws IOException, InterruptedException {
        
        //Split title from body and strip tags
        String[] titleAndText = parsePage(page);
        String titleString = titleAndText[0];
        String bodyString = titleAndText[1];

        //If the title is invalid, then scrap the page
        if(isInvalidPageTitle(titleString)) return;

        //Sanitize the title
        Text title = new Text(titleString.replace(' ', '_'));

        //Parse the body of the page for links
        Matcher links = linksSyntax.matcher(bodyString);
        
        //Loop through the found links
        while (links.find()) {
            String otherPage = links.group();
            otherPage = parseLink(otherPage);
            //If the link is not a valid link to a wiki page then discard it
            if(otherPage == null || otherPage.isEmpty()) continue;
            //Add valid linked pages to the Map
            context.write(title, new Text(otherPage));
        }
    }
    


    private String parseLink(String link){
        if(isInvalidLink(link)) return null;

        int linkStart = 0;
        int linkEnd = link.length();

        int pipePosition = link.indexOf("|");
        linkEnd = (pipePosition > linkStart) ? pipePosition : linkEnd;

        int part = link.indexOf("#");
        linkEnd = (part > linkStart) ? part : linkEnd;

        //Sanitize the link
        link = link.substring(linkStart, linkEnd);
        link = link.replaceAll("\\s", "_");
        link = link.replaceAll(",", "");
        link = (link.contains("&amp;")) ? link.replace("&amp;", "&") : link;
        
        return link;
    }

    private String[] parsePage(Text page) throws CharacterCodingException {
        String[] titleAndBody = new String[2];
        
        int startTitle = page.find("<title>");
        int endTitle = page.find("</title>", startTitle);

        int startBody = page.find("<text");
        //Find opening <text> tag, excluding potential attributes
        startBody = page.find(">", startBody);
        int endBody = page.find("</text>", startBody);

        if(startTitle == -1 || endTitle == -1 || startBody == -1 || endBody == -1) {
            return new String[]{"",""};
        }

        //Modify starting indices to account for the tags themselves, as we don't want them
        startTitle += 7;
        startBody += 1;

        //Parse the Text to Strings
        titleAndBody[0] = Text.decode(page.getBytes(), startTitle, endTitle-startTitle);
        titleAndBody[1] = Text.decode(page.getBytes(), startBody, endBody-startBody);
        
        return titleAndBody;
    }

    private boolean isInvalidPageTitle(String titleString) {
        return titleString.contains(":");
    }

    private boolean isInvalidLink(String link) {
        int minLength = 1;
        int maxLength = 100;

        if( link.length() < minLength || link.length() > maxLength) return true;
        char firstChar = link.charAt(0);
        
        if( firstChar == '#' || firstChar == ',' || firstChar == '.' || firstChar == '&' || firstChar == '\'' ||
                firstChar == '-' || firstChar == '{' || firstChar =='|') return true;
        
        return ( link.contains(":") || link.contains(",") ||
                (link.indexOf('&') > 0) && !(link.substring(link.indexOf('&')).startsWith("&amp;")));
    }
}
