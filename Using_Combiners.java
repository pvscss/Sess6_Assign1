package Task8;

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;

public class Using_Combiners {

	public static void main(String[] args) throws Exception 
	{
		if (args.length != 2) 
			{
		      System.err.println("Usage: WordCount <input path> <output path>");
		      System.exit(-1);
		    }
		//Job Related Configurations
		Configuration conf=new Configuration();
		Job job=new Job(conf,"Invalid Records");
		job.setJarByClass(Using_Combiners.class);
		
		//Provide paths to pick the input file for the job
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		
		//Provide paths to pick the output file for the job, and delete it if already present
		Path OutputPath=new Path(args[1]);
		FileOutputFormat.setOutputPath(job, OutputPath);
		OutputPath.getFileSystem(conf).delete(OutputPath,true);
		
		
		//To set the mapper and reducer of this job
		job.setMapperClass(Mappers.class);
		job.setCombinerClass(Combiners_Reducers.class);
		job.setReducerClass(Combiners_Reducers.class);
		
		//set the input and output format class
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		//Set mapper key-value class types
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
				
				
		//Set output key'value class types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		//Execute the Job
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
