package uk.ac.ncl.cs.csc8101.hadoop.parse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class PageLinksParseReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String initialPagerank = "1.0\t";
        String stored = initialPagerank;
        boolean first = true;

        for (Text value : values) {
            if(!first) stored += ",";
            stored += value.toString();
            first = false;
        }
        //Reducer stores pages and outgoing links in the format: "[page]  [initialPagerank]   outLinkA,outLinkB,outLinkC..."
        context.write(key, new Text(stored));
    }
}
