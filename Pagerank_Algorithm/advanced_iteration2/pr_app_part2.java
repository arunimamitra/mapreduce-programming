import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class pr_app_part2 {
    
    public static void main(String[] args) throws Exception {
        final Configuration config = new Configuration();
        config.set("mapred.textoutputformat.separator", " ");
	FileSystem fileSystem = FileSystem.get(config);

	for(int i=0;i<3;i++){
        
//		System.out.println("Running job for iter="+(i+1));
		Job job = Job.getInstance(config);
		job.setJobName("iteration" + i + "");
		
		job.setJarByClass(pr_app_part2.class);
        	job.setNumReduceTasks(1);       // limit number of reducers to 1
        	job.setMapperClass(pr_map.class);
        	job.setReducerClass(pr_reduce.class);
        	job.setOutputKeyClass(Text.class);
        	job.setOutputValueClass(Text.class);
		Path outputFilePath = new Path(args[0]+"iteration_"+(i+1));

		if (fileSystem.exists(outputFilePath)) fileSystem.delete(outputFilePath, true);
        	Path inputFilePath = new Path(args[0]+"iteration_"+i+"/"); 
        	FileInputFormat.addInputPath(job, inputFilePath);
        	FileOutputFormat.setOutputPath(job, outputFilePath);

		job.waitForCompletion(true); // Important! Otherwise the MR job wouldn't run
	}
	//      System.out.println("Job complete");
    }
}
