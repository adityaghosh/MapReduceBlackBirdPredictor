package com.project;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

/*
 * @author Aditya Ghosh
 * This Class performs initial data cleaning.
 */
public class PreProcess {
	public static class PreProcessMapper extends Mapper<Object, Text, NullWritable, Text> {
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
				// Remove Unwanted Columns
				/*StringTokenizer itr = new StringTokenizer(value.toString(), ","); 
				int count = 0;
				ArrayList<String> result = new ArrayList<String>();
				while(itr.hasMoreTokens()){
					String x = itr.nextToken();
					if (count<15){
						result.add(x);
					}
					if (count>=952){
						result.add(x);
					}
					count += 1; 
				}
				String r = StringUtils.join(",", result);
				context.write(NullWritable.get(), new Text(r));*/
				//Using For loop
			
				int[] indexes = {1,9,10,11,12,13,14,960,961,963,966,967};
				ArrayList<String> result = new ArrayList<String>();
				String[] split = value.toString().split(",");
				// If any column has '?' we do not include it as a row 
				boolean hasQ = false;
				for (int i:indexes){
					if (split[i].equalsIgnoreCase("?"))
						hasQ = true;
					result.add(split[i]);
				}
				if (hasQ == false){
					// Add Agelaius_phoeniceus
					if (split[26] != "0" && !split[26].equalsIgnoreCase("Agelaius_phoeniceus"))
						result.add("1");
					else
						result.add(split[26]);
					String r = StringUtils.join(",", result);
					context.write(NullWritable.get(), new Text(r));
				}
				/*
				result.add(split[1]);
				for (int i=9; i<15; i++)
					result.add(split[i]);
				// Changing counts tobinary yes and no for Agelaius_phoeniceus
				if (split[26] != "0" && split[26] !="Agelaius_phoeniceus")
					result.add("1");
				else
					result.add(split[26]);
				// Skipping all other species next data column is at index 952
				result.add(split[960]);
				result.add(split[961]);                              
				result.add(split[963]);
				result.add(split[966]);
                result.add(split[967]);
				/*for (int i=960; i<968; i++)
					result.add(split[i]);*/
				//String r = StringUtils.join(",", result);
				//context.write(NullWritable.get(), new Text(r));
		}
	}
}
