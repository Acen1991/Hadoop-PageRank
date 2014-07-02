
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;

public class MatrixVectorMult {
	
	static class FirstMap extends Mapper<LongWritable, Text, Text, Text> {
		private Text row = new Text();
		private Text column = new Text();
		private Text element = new Text();
		protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException
			{
				
			String[] values  = value.toString().split("\\s+");
				if(values.length == 2)
				{
					row.set(values[0]);
					element.set (values[1]);
					context.write(row, element);
				}
				else if(values.length == 3)
				{
					element.set (values[0]);
					column.set (values[1]);
					element.set(element + " " + values[2]);
					context.write(column, element);
				}
			
			}
		}
	

	static class FirstReduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable partialResult = new DoubleWritable ();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			HashMap<Integer,Double> matrixColumn = new HashMap<Integer,Double> ();
			double vectorValue = 0.;
			
			for (Text value : values)
			{
				String[] elements  = value.toString().split("\\s+");
				if(elements.length == 1)
				{
					vectorValue = Double.valueOf(elements[0]);
				}
				else if(elements.length == 2)
				{
					matrixColumn.put(Integer.valueOf(elements[0]),
					                 Double.valueOf(elements[1]));
				}
			}
			
			for (Integer row:matrixColumn.keySet())
			{
				partialResult = new DoubleWritable(matrixColumn.get(row)* vectorValue);
				context.write(new Text(row.toString()), partialResult);
			}
		}
	}
	static class SecondMap extends Mapper<Text, Text, Text, Text> {
		
		protected void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException
			{
				
				context.write(key,value);
			
			}
		}
	
	static class CombinerForSecondMap extends Reducer<Text, Text, Text, Text> {
		private Text partialResult = new Text();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			Double vectorValue = 0.;
			
			for (Text value : values)
			{
				vectorValue += Double.valueOf(value.toString());
			}
			partialResult.set(vectorValue.toString());
			context.write(key, partialResult);
		}
	}
	static class SecondReduce extends Reducer<Text, Text, Text, DoubleWritable> {
		private DoubleWritable finalResult = new DoubleWritable ();
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			double vectorValue = 0.;
			
			for (Text value : values)
			{
				vectorValue += Double.valueOf(value.toString());
			}
			finalResult.set(vectorValue);
			context.write(key, finalResult);
		}
	}
	public static void multiplicationJob(Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		String initialVector = conf.get("initialPageRankPath");
		String currentVector = conf.get("currentPageRankPath");
		String intermediarySum = "data/intermediarySum"; 
		String matrix = conf.get("stochasticMatrixPath");
		FileUtils.deleteDirectory(new File(intermediarySum));
		
		Job job1 = Job.getInstance(conf);
		job1.setJobName("multiplication first job");
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);

		job1.setMapperClass(FirstMap.class);
		job1.setReducerClass(FirstReduce.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job1, initialVector + "," + matrix);
		FileOutputFormat.setOutputPath(job1, new Path(intermediarySum));

		job1.waitForCompletion(true);
		
		// Second job
		Job job2 = Job.getInstance(conf);
		job2.setJobName("multiplication second job");
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setMapperClass(SecondMap.class);
		job2.setReducerClass(SecondReduce.class);
		job2.setCombinerClass(CombinerForSecondMap.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job2, new Path(intermediarySum));
		FileOutputFormat.setOutputPath(job2, new Path(currentVector));

		job2.waitForCompletion(true);
		FileUtils.deleteDirectory(new File(intermediarySum));
	}
	
}

