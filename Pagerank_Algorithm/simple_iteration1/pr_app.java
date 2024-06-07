import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class pr_app {
    
    public static void main(String[] args) throws Exception {
	if(args.length<2) {
            System.out.println("Missing arguments\nUsage: pr_app [input file] [output file]");
            System.exit(-1);
        }
        
        final Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", " ");
        Job job = Job.getInstance(conf);

        job.setJarByClass(pr_app.class);
        job.setNumReduceTasks(1);	// limit number of reducers to 1
        job.setMapperClass(pr_map.class);
        job.setReducerClass(pr_reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	Path inputFilePath = new Path(args[0]), outputFilePath = new Path(args[1]);
	FileInputFormat.addInputPath(job, inputFilePath);
	FileOutputFormat.setOutputPath(job, outputFilePath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
