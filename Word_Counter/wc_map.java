import java.io.IOException;
import java.util.*;
     
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class wc_map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable write_one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    StringTokenizer itr_t = new StringTokenizer(value.toString().toLowerCase());    // line iterator

    while (itr_t.hasMoreTokens()) {
	String currToekn=itr_t.nextToken();     // checking validity of token word starts here

	String finalToken=currToekn.replaceAll("[^a-zA-Z0-9]", "");     // replacing all punctuation marks, non-alphanumeric characters
	if(finalToken.matches("[a-zA-Z]+")){        // checker to see if word is alphabetic or not
        	word.set(finalToken);
        	context.write(word, write_one);             // mapper will only write alphabetic words into intermediate file
	}
    }
    }
 }
