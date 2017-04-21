package com.birdpredictor;

import java.util.ArrayList;

import org.apache.hadoop.util.StringUtils;

public class DataLineParser {
	public static String get(String line){
		int[] indexes = {1,9,10,11,12,13,14,960,961,963,966,967};
		ArrayList<String> result = new ArrayList<String>();
		String[] split = line.split(",");
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
			return r;
		}
		return "";
	}
}
