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

public class wc_app{
   public static void main(String[] args) throws Exception {
    Job job = Job.getInstance();
    job.setJarByClass(wc_app.class);
    job.setNumReduceTasks(1); // limit number of reduce tasks to 1

    // setting mapper, combiner and reducer class
    job.setMapperClass(wc_map.class);
    job.setCombinerClass(wc_reduce.class);
    job.setReducerClass(wc_reduce.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1); 
 

   }
}

