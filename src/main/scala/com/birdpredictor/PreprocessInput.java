package com.birdpredictor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.util.StringUtils;

/**
 * @author Aditya Ghosh
 * This Class is used to process any input from the bird dataset. 
 */
public class PreprocessInput {
	private static HashMap<Integer,HashMap<String, Integer>> categoricalHashMap = new HashMap<Integer,HashMap<String, Integer>>();
	private static HashMap<Integer,Integer> categoricalCounters = new HashMap<Integer,Integer>();
	
	/**
	 * @param line : A single comma separated line from input
	 * @param islabeledData : True iff the the line is from 
	 * @param categoricalIndexes : A list of integer indexes for categorical variables
	 * @param continuousIndexes : A list of integer indexes for continuous variables
	 * @return : A sparse Matrix row representation
	 */
	public static String preprocess(String line, boolean islabeledData, List<Integer> categoricalIndexes, List<Integer> continuousIndexes){
		String[] split = line.split(",");
		ArrayList<String> sparseValues = new ArrayList<String>();
		int counter = 1;
		boolean isColumnHeader = split[26].equalsIgnoreCase("Agelaius_phoeniceus");
		if (!isColumnHeader) {
			if (islabeledData){
				if (!split[26].equalsIgnoreCase("0"))
					sparseValues.add("1");
				else
					sparseValues.add("0");
			}
			else {
				sparseValues.add("0");
			}
			for (int i : categoricalIndexes){
				if (!split[i].equalsIgnoreCase("?")) {
					if(categoricalHashMap.containsKey(i)){
						if (categoricalHashMap.get(i).containsKey(split[i])) {
							// Category exists
							sparseValues.add(Integer.toString(counter)+":"+Integer.toString(categoricalHashMap.get(i).get(split[i])));
						}
						else {
							// Creating New Category on this categorical set
							int newCounter = categoricalCounters.get(i)+1;
							categoricalHashMap.get(i).put(split[i], newCounter);
							categoricalCounters.put(i,newCounter);
							sparseValues.add(Integer.toString(counter)+":"+Integer.toString(newCounter));
						}
					}
					else {
						// Creating a new Category set
						categoricalHashMap.put(i, new HashMap<String, Integer>());
						categoricalHashMap.get(i).put(split[i], 0);
						categoricalCounters.put(i, 0);
						sparseValues.add(Integer.toString(counter)+":0");
					}
				}
				else{
					// Value is ?
				}
				counter += 1;
			}
			
			for (int i:continuousIndexes){
				if (!split[i].equalsIgnoreCase("?")) {
					String toadd = split[i].replaceAll("[^\\d.]", "");
					if (toadd.length()>0)
						sparseValues.add(Integer.toString(counter)+":"+toadd);
				}
				else{
					// Value is ?
				}
				counter += 1;
			}
			if (sparseValues.size() <30){
				String r = String.join(" ", sparseValues);
				return r;
			}
			return "";
		}
		return "";
	}
}
