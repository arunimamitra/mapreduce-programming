import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class wc_reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
 
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
        int ctr = 0;    // calculates total number of words present in document for every key
    	for (IntWritable value : values) {
        	ctr=ctr+value.get();
    	}
    context.write(key, new IntWritable(ctr));
    }
 }
