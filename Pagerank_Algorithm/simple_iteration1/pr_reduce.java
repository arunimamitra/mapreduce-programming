import java.util.*;
import java.io.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

public class pr_reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double final_probability=0.0;
        String final_output="";
        for (Text value : values) {
            // for Pair 1 <dest_pg, contribution> , add probability
	if (value.toString().matches("\\d.*")) final_probability += Double.parseDouble(value.toString());
            // for Pair 2 <src_pg, outbound links>
        else final_output += value.toString();
		}
		final_output+=String.valueOf(final_probability);
        context.write(key, new Text(final_output));
    }
}
