package com.tcs.sainsburys;

/**
 * 
 * AverageNumberOfWordsReducer.java
 * This is a Reduce program to calculate the average
 * 
 * NOTE:  Please us this reduce method commented in case you dont want to use a combiner in between
 * 
 **/
 import java.io.IOException;
import java.util.Iterator;

 import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

 public class AverageNumberOfWordsReducer
 extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	 @Override
	 public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			 throws IOException, InterruptedException {

		 float totalRecords=0, totalWords=0;
		 Iterator<FloatWritable> iterator = values.iterator();
		 while (iterator.hasNext()) {
			 totalWords+=Float.parseFloat(iterator.next().toString());
			 totalRecords++;
		 }
		 System.out.println("Total words : "+totalWords+" total records "+totalRecords);
		 
		 float averageNumberOfWords = totalWords/totalRecords;
		 
		 //Write output
		 context.write(new Text("AverageWordsAcrossEmails"), new FloatWritable(averageNumberOfWords));
	 }
	 
 }
