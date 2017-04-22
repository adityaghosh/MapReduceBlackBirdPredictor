package com.birdpredictor;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.util.StringUtils;

public class DataLineParser {
	private static HashMap<String, Long> State_Province = new HashMap<String,Long>();
	private static HashMap<String, Long> County = new HashMap<String,Long>();
	private static long state_province_counter = 0;
	private static long county_counter = 0;
	
	public static String get(String line){
		//int[] indexes = {1,4,5,7,9,10,11,12,13,14,16,958,959,960,961,962,963,966,967};
		int[] indexes = {1,12,13,14,960,961,963,966,967};
		
		ArrayList<String> result = new ArrayList<String>();
		String[] split = line.split(",");
		// If any column has '?' we do not include it as a row
		boolean hasQ = false;
		int columnNumber = 0;
		// If row not Title row
		if (!split[26].equalsIgnoreCase("Agelaius_phoeniceus")) {
			// Add Agelaius_phoeniceus
			if (!split[26].equalsIgnoreCase("0"))
				result.add("1");
			else
				result.add(split[26]);
			for (int i:indexes){
				// If any column has '?' Ignore the complete row
				if (split[i].equalsIgnoreCase("?")){
					hasQ = true;
					break;
				}
				switch(i){
				 	case 1:
				 		split[i] = split[i].replaceAll("[^\\d.]", "");
				 		break;
				 	case 9:
				 		if (State_Province.containsKey(split[i])){
				 			split[i] = Long.toString(State_Province.get(split[i].trim()));
				 		}
				 		else {
				 			state_province_counter += 1;
				 			State_Province.put(split[i], state_province_counter);
				 			split[i] = Long.toString(state_province_counter);
				 		}
				 		break;
				 	case 10:
				 		if (County.containsKey(split[i])){
				 			split[i] = Long.toString(County.get(split[i]));
				 		}
				 		else {
				 			county_counter += 1;
				 			County.put(split[i], county_counter);
				 			split[i] = Long.toString(county_counter);
				 		}
				 		break;
				 	case 11:
				 		split[i] = split[i].replaceAll("[^\\d.]", "");
				 		break;
				 	case 961:
				 		split[i] = split[i].replaceAll("[^\\d.]", "");
				 		break;
				 	case 962:
				 		split[i] = split[i].replaceAll("[^\\d.]", "");
				 		break;
			 		default:
			 			break;
				}
				
				columnNumber += 1;
				result.add(Integer.toString(columnNumber)+":"+split[i]);
			}
			if (hasQ == false){
				// Add NLCD Data
				/*for (int y = 968;y<=1015;y++){
					columnNumber += 1;
					result.add(Integer.toString(columnNumber)+":"+split[y]);
				}*/
				String r = StringUtils.join(" ", result);
				return r;
			}
		}
		return "";
	}
}
