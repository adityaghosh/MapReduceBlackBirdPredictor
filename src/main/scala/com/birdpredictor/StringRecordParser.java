package com.birdpredictor;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.util.StringUtils;
/**
 * 
 * @author rohitkumar
 *
 */
public class StringRecordParser {
	private static HashMap<String, Long> stateMap = new HashMap<String, Long>();
	private static HashMap<String, Long> countyMap = new HashMap<String, Long>();
	private static long stateCounter = 0;
	private static long countyCounter = 0;

	public static String get(String value) {
		// int[] indexes =
		// {1,4,5,7,9,10,11,12,13,14,16,958,959,960,961,962,963,966,967};
		// int[] indexes = { 1, 12, 13, 14, 960, 961, 963, 966, 967 };
		if (value.toString().contains("YEAR"))
			return "";

		String record = value.toString();
		String sampling_event_id = record.substring(0, record.indexOf(","));

		String[] files = record.split(sampling_event_id);

		String checklist = files[1];
		String core = files[2];
		String extended = files[3];

		String checklistFormatted = getChecklist(checklist);
		String coreFormatted = getCore(core);
		String extendedFormatted = getExtended(extended);

		if (checklistFormatted.trim().isEmpty())
			return "";

		String output = checklistFormatted + coreFormatted + extendedFormatted;

		return output.replace(",", " ");
	}

	private static String getExtended(String extended) {
		String[] extendedColums = extended.split(",");

		String distFromFlowingWater = extendedColums[74];
		String distInFlowingWater = extendedColums[75];
		String distFromStandingFresh = extendedColums[76];
		String distInStandingFresh = extendedColums[77];
		String distFromWetVeg = extendedColums[78];
		String distInWetVeg = extendedColums[79];

		StringBuilder sb = new StringBuilder();

		if (!distFromFlowingWater.trim().isEmpty() && !distFromFlowingWater.contains("?")
				&& !distFromFlowingWater.contains("X")) {
			sb.append(",958:" + distFromFlowingWater);
		}

		if (!distInFlowingWater.trim().isEmpty() && !distInFlowingWater.contains("?")
				&& !distInFlowingWater.contains("X")) {
			sb.append(",959:" + distInFlowingWater);
		}

		if (!distFromStandingFresh.trim().isEmpty() && !distFromStandingFresh.contains("?")
				&& !distFromStandingFresh.contains("X")) {
			sb.append(",960:" + distFromStandingFresh);
		}

		if (!distInStandingFresh.trim().isEmpty() && !distInStandingFresh.contains("?")
				&& !distInStandingFresh.contains("X")) {
			sb.append(",961:" + distInStandingFresh);
		}

		if (!distFromWetVeg.trim().isEmpty() && !distFromWetVeg.contains("?") && !distFromWetVeg.contains("X")) {
			sb.append(",962:" + distFromWetVeg);
		}

		if (!distInWetVeg.trim().isEmpty() && !distInWetVeg.contains("?") && !distInWetVeg.contains("X")) {
			sb.append(",963:" + distInWetVeg);
		}

		return sb.toString().trim();
	}

	private static String getCore(String core) {
		String[] coreColums = core.split(",");

		String elev_ned = coreColums[6];
		String bcr = coreColums[7];
		String avgTemp = coreColums[10];
		String precipitation = coreColums[13];
		String snow = coreColums[14];

		StringBuilder sb = new StringBuilder();

		if (!elev_ned.trim().isEmpty() && !elev_ned.contains("?") && !elev_ned.contains("X")) {
			sb.append(",953:" + elev_ned);
		}

		if (!bcr.trim().isEmpty() && !bcr.contains("?") && !bcr.contains("X")) {
			sb.append(",954:" + bcr);
		}

		if (!avgTemp.trim().isEmpty() && !avgTemp.contains("?") && !avgTemp.contains("X")) {
			sb.append(",955:" + avgTemp);
		}

		if (!precipitation.trim().isEmpty() && !precipitation.contains("?") && !precipitation.contains("X")) {
			sb.append(",956:" + precipitation);
		}

		if (!snow.trim().isEmpty() && !snow.contains("?") && !snow.contains("X")) {
			sb.append(",957:" + snow);
		}

		return sb.toString().trim();
	}

	public static String getChecklist(String checklist) {
		String[] checklistColums = checklist.split(",");

		String year = checklistColums[4];

		if (year.equals("?"))
			return "";

		Long yearVal = Long.parseLong(year);
		String month = checklistColums[5];
		String day = checklistColums[6];
		String time = checklistColums[7];
		String state = checklistColums[9];
		String county = checklistColums[10];

		String stateVal = !state.equals("?") ? getValFor(stateMap, state.trim(), stateCounter) : "?";
		String countyVal = !county.equals("?") ? getValFor(countyMap, county.trim(), countyCounter) : "?";

		if (month.equals("?") || day.equals("?"))
			return "";

		Integer monthVal = Integer.parseInt(month);
		Integer dayVal = Integer.parseInt(day);
		Double timeVal = Double.parseDouble(time);

		String count_type = checklistColums[11];
		Integer countTypeVal = 7;

		if (!count_type.equals("?")) {
			countTypeVal = Integer.parseInt("" + count_type.charAt(2)) + 1;
			if (countTypeVal > 6)
				countTypeVal = 7;
		}

		String effort_hrs = checklistColums[12];
		String effort_dist = checklistColums[13];
		String num_of_observers = checklistColums[16];

		if (effort_hrs.equals("?"))
			effort_hrs = "0.5";

		if (effort_dist.equals("?"))
			effort_dist = "0";

		if (num_of_observers.equals("?"))
			num_of_observers = "1";

		String agelaius_phoeniceus = checklistColums[26];

		Integer agelaius_phoeniceusVal = 0;

		if (agelaius_phoeniceus.equals("?"))
			agelaius_phoeniceusVal = 0;
		else if (agelaius_phoeniceus.equalsIgnoreCase("X"))
			agelaius_phoeniceusVal = 1;
		else if (!agelaius_phoeniceus.equals("0"))
			agelaius_phoeniceusVal = 1;

		String speciesString = "";

		for (int i = 19; i <= 952; i++) {
			if (i == 26) {
				continue;
			}

			String specie = checklistColums[i];
			Integer specieVal = 0;

			if (specie.equals("?"))
				specieVal = 0;
			else if (specie.equalsIgnoreCase("X"))
				specieVal = 1;
			else if (!specie.equals("0"))
				specieVal = 1;

			if (specieVal > 0)
				speciesString = speciesString + i + ":" + 1 + ",";
		}

		if (speciesString.length() > 2)
			speciesString = speciesString.substring(0, speciesString.length() - 1);

		StringBuilder sb = new StringBuilder();

		sb.append(agelaius_phoeniceusVal);
		if (!countyVal.equals("0") && !countyVal.equals("?") && countyVal.length()!=0) {
			sb.append(",9:" + countyVal);
		}
		if (!stateVal.equals("0") && !stateVal.equals("?") && stateVal.length()!=0) {
			sb.append(",10:" + stateVal);
		}
		if (yearVal > 0) {
			sb.append(",2:" + yearVal);
		}

		if (monthVal > 0) {
			sb.append(",3:" + monthVal);
		}

		if (dayVal > 0) {
			sb.append(",4:" + dayVal);
		}

		if (timeVal > 0) {
			sb.append(",5:" + timeVal);
		}

		if (countTypeVal > 0) {
			sb.append(",6:" + countTypeVal);
		}
		if (Double.parseDouble(effort_hrs) > 0.0) {
			sb.append(",7:" + effort_hrs);
		}
		if (Double.parseDouble(effort_dist) > 0.0) {
			sb.append(",8:" + effort_dist);
		}
		if (Double.parseDouble(num_of_observers) > 0.0) {
			sb.append(",9:" + num_of_observers);
		}

		if (!speciesString.trim().isEmpty()) {
			sb.append(",");
			sb.append(speciesString);
		}

		return sb.toString();

	}

	private static String getValFor(HashMap<String, Long> myMap, String data, Long counter) {
		if (myMap.containsKey(data)) {
			Long val = myMap.get(data);
			return Long.toString(val);
		} else {
			counter += 1;
			myMap.put(data, counter);
			return Long.toString(counter);
		}
	}
}

//
// ArrayList<String> result = new ArrayList<String>();
// String[] split = line.split(",");
// // If any column has '?' we do not include it as a row
// boolean hasQ = false;
// int columnNumber = 0;
// // If row not Title row
// if (!split[26].equalsIgnoreCase("Agelaius_phoeniceus")) {
// // Add Agelaius_phoeniceus
// if (!split[26].equalsIgnoreCase("0"))
// result.add("1");
// else
// result.add(split[26]);
// for (int i:indexes){
// // If any column has '?' Ignore the complete row
// if (split[i].equalsIgnoreCase("?")){
// hasQ = true;
// break;
// }
// switch(i){
// case 1:
// split[i] = split[i].replaceAll("[^\\d.]", "");
// break;
// case 9:
// if (State_Province.containsKey(split[i])){
// split[i] = Long.toString(State_Province.get(split[i].trim()));
// }
// else {
// state_province_counter += 1;
// State_Province.put(split[i], state_province_counter);
// split[i] = Long.toString(state_province_counter);
// }
// break;
// case 10:
// if (County.containsKey(split[i])){
// split[i] = Long.toString(County.get(split[i]));
// }
// else {
// county_counter += 1;
// County.put(split[i], county_counter);
// split[i] = Long.toString(county_counter);
// }
// break;
// case 11:
// split[i] = split[i].replaceAll("[^\\d.]", "");
// break;
// case 961:
// split[i] = split[i].replaceAll("[^\\d.]", "");
// break;
// case 962:
// split[i] = split[i].replaceAll("[^\\d.]", "");
// break;
// default:
// break;
// }
//
// columnNumber += 1;
// result.add(Integer.toString(columnNumber)+":"+split[i]);
// }
// if (hasQ == false){
// // Add NLCD Data
// /*for (int y = 968;y<=1015;y++){
// columnNumber += 1;
// result.add(Integer.toString(columnNumber)+":"+split[y]);
// }*/
// String r = StringUtils.join(" ", result);
// return r;
// }
// }
// return "";
// }
// }
