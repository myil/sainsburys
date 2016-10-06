package com.tcs.sainsburys;

/**
 * This is a driver program to find the average number of words across mails.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageNumberOfWordsDriver {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Missing Input and Output arguments ... <Input folder> <output folder>");
			System.exit(-1);
		}

		//Define job
		Job job = new Job();
		job.setJarByClass(AverageNumberOfWordsDriver.class);
		job.setJobName("AverageNumberOfWords");

		//Set input and output locations
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Set Input and Output formats
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    //Set Mapper and Reduce classes
		job.setMapperClass(AverageNumberOfWordsMapper.class);
		job.setReducerClass(AverageNumberOfWordsReducer.class);
		
		//Output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		//Submit job
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
