/*The MIT License (MIT)
 
 Copyright (c) 2015 Sandeep Raveendran Thandassery
 
 Permission is hereby granted, free of charge, to any person obtaining a copy
 of this software and associated documentation files (the "Software"), to deal
 in the Software without restriction, including without limitation the rights
 to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 copies of the Software, and to permit persons to whom the Software is
 furnished to do so, subject to the following conditions:
 
 The above copyright notice and this permission notice shall be included in
 all copies or substantial portions of the Software.
 
 THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 THE SOFTWARE.
 */
package edu.cse.analyser.components;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Configuration file for the MapReduce program. Used to set different
 * configuration parameters for the jobs. Also the starting point of the
 * application.
 */
public class TemperaturePattern extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{
		int resp = ToolRunner.run(new TemperaturePattern(), args);
		System.exit(resp);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		if (args.length != 3)
		{
			System.err.println("Usage: *.jar <input path> <output path> <monthly|seasonal>");
			System.exit(-1);
		}
		Configuration conf = getConf();
		
		// -- Job creation
		Job job = new Job(conf, "Temperature Anaylser Job");
		job.setJarByClass(TemperaturePattern.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// -- Setup output key/values class type
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// -- Setup MapReduce job. If passed argument is seasonal,
		// -- executes Mapper to generate seasonal output else execute
		// -- Mapper for generating monthly data. Reducer for both remains
		// -- same.
		if ("seasonal".equalsIgnoreCase(args[2]))
		{
			job.setMapperClass(SeasonalTempMapper.class);
		}
		else
		{
			job.setMapperClass(MonthlyTempMapper.class);
		}
		job.setReducerClass(TempReducer.class);
		// -- Execute job and return status
		return job.waitForCompletion(true) ? 0 : 1;
	}
}