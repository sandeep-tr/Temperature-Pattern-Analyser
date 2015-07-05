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

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for processing seasonal temperature data.
 */
public class SeasonalTempMapper extends Mapper<Object, Text, Text, LongWritable>
{

	private static final String[] SEASONS = { "Fall", "Spring", "Summer", "Autumn" };
	private static final int YEAR = 0;
	private static final int MONTH = 1;
	private static final int TEMPERATURE = 4;
	private Text keyWord = new Text();
	private LongWritable tempWritable = new LongWritable();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException,
			InterruptedException
	{
		String[] values = value.toString().split("\\s+");
		// -- generate key from month and year provided.
		// -- also discard temperature with values -9999 (indicates no captured
		// data for that entry)
		String newKey = createKey(values[MONTH], values[YEAR]);
		long temperature = Long.parseLong(values[TEMPERATURE]);
		if (newKey != null && temperature != -9999)
		{
			keyWord.set(newKey);
			tempWritable.set(temperature);
			context.write(keyWord, tempWritable);
		}
	}

	/**
	 * Generates a key based on the month and year provided.
	 *
	 * @param month
	 * @param year
	 * @return season based key
	 */
	private String createKey(String month, String year)
	{
		String key = null;
		int monthNo = Integer.parseInt(month);
		if ((monthNo >= 1 && monthNo <= 2) || monthNo == 12)
		{
			key = new StringBuilder(SEASONS[0]).append(" ").append(year).toString();
		}
		else if (monthNo >= 3 && monthNo <= 5)
		{
			key = new StringBuilder(SEASONS[1]).append(" ").append(year).toString();
		}
		else if (monthNo >= 6 && monthNo <= 8)
		{
			key = new StringBuilder(SEASONS[2]).append(" ").append(year).toString();
		}
		else if (monthNo >= 9 && monthNo <= 11)
		{
			key = new StringBuilder(SEASONS[3]).append(" ").append(year).toString();
		}
		return key;
	}
}