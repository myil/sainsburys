package com.tcs.sainsburys;

/**
 * This is a Mapper program
 */

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageNumberOfWordsMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String record = value.toString();
		String[] fields = record.split(",");
		String body = fields[2];
		int wordCount = countwords(body);		
		context.write(new Text("1"), new FloatWritable(wordCount));
		
	}
	
	private int countwords(String body) {
		int returnValue = 0;
		if (body!=null) {
			StringTokenizer stTokens = new StringTokenizer(body, " ");
			returnValue =  stTokens.countTokens();
		}
		return returnValue;
	}
}
