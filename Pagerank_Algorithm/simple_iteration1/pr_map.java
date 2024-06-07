import java.io.*;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class pr_map extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens=value.toString().split("\\s");
		
		double probability = Double.parseDouble(tokens[tokens.length-1]);	// total probability
		double contributionCalculated = probability/(tokens.length-2);		// individual contribution og linked page
		
		String links="";
		for(int i=1;i<tokens.length-1; ++i) {
			links+=tokens[i]+" ";
			context.write(new Text(tokens[i]), new Text(String.valueOf(contributionCalculated)));
		}
		context.write(new Text(tokens[0]), new Text(links));

		
	}
}
