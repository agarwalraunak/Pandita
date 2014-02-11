package com.pandita.controller;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import com.pandita.service.PanditaService;
import com.pandita.util.DataLoadUtil;

/**
 * Handles requests for the application home page.
 */
@Controller
public class AirlinePerformancePerYear {
	
	private @Autowired PanditaService panditaService;
	private @Autowired DataLoadUtil dataLoadUtil;
	
	@RequestMapping(value="airlinePerformace.htm", method=RequestMethod.GET)
	public String displayForm(Model model){
		
		Set<String> rowKeys = panditaService.retrieveRowKeyForAirlineColumnFamily();
		
		model.addAttribute("airlines", rowKeys);
		return "airlinePerformance";
	}
	
	/**
	 * Simply selects the home view to render by returning its name.
	 */
	@RequestMapping(value="airlinePerformace.htm", method = RequestMethod.POST)
	public String home(HttpServletRequest request, Model model) {
		
		String startYearStr = request.getParameter("startRange");
		String endYearStr = request.getParameter("endRange");
		String[] airlines = request.getParameterValues("airlines");
		
		Set<String> rowKeys = panditaService.retrieveRowKeyForAirlineColumnFamily();
		model.addAttribute("airlines", rowKeys);
		int startYear, endYear;
		try{
			startYear = Integer.parseInt(startYearStr);
			endYear= Integer.parseInt(endYearStr);
		}
		catch(Exception e){
			model.addAttribute("Error", "Invalid Input");
			return "airlinePerformance"; 
		}
		
		if (airlines == null || airlines.length == 0){
			model.addAttribute("Error", "Please select airlines!");
			return "airlinePerformance";
		}
		
		String[] years = new String[endYear - startYear+1];
		int j = 0;
		for (int i = startYear; i<= endYear; i++){
			years[j] = String.valueOf(i);
			++j;
		}
		
		Map<String, Map<String, String>> data = panditaService.retrieveAirlinePerformancePerYear(years);
		
		model.addAttribute("data", data);
		
		model.addAttribute("reqAirlines", airlines);
		model.addAttribute("reqYears", years);
		return "airlinePerformance";
	}
	
	@RequestMapping(value="predictionEngine.htm", method=RequestMethod.GET)
	public String machineLearningForm(){
		
		
		
		return "machineLearningForm";
	}
	
	@RequestMapping(value="predictionEngineCompute.htm", method=RequestMethod.GET)
	public String submitMachineLearningForm(HttpServletRequest request, Model model){

		String dayStr = request.getParameter("day");
		String monthStr = request.getParameter("month");
		String departureTimeStr = request.getParameter("departureTime");
		String carrierStr = request.getParameter("carrier");
		String airportsStr = request.getParameter("airports");
		String arrivalDelayStr = request.getParameter("arrivalDelay");
		
		int day, month, departureTime, carrier, arrivalDelay;
		try{
			day = Integer.parseInt(dayStr);
			month = Integer.parseInt(monthStr);
			departureTime = Integer.parseInt(departureTimeStr);
			arrivalDelay = Integer.parseInt(arrivalDelayStr);
		} catch(NumberFormatException e){
			model.addAttribute("error", "Invalid Input");
			return "machineLearningForm";
		}
		
		if (carrierStr == null || airportsStr == null || carrierStr.trim().isEmpty() || airportsStr.trim().isEmpty()){
			model.addAttribute("error", "Invalid Input");
			return "machineLearningForm";
		}
		
		BufferedReader fileData = null;
		try {
			fileData = dataLoadUtil.readFile("/json_res/"+airportsStr+".json");
		} catch (FileNotFoundException e) {
			model.addAttribute("error", "System has encountered some, technical issues please come back later!");
			return "machineLearningForm";
		}
		
		if (fileData == null){
			model.addAttribute("error", "System has encountered some, technical issues please come back later!");
			return "machineLearningForm";
		}
		
		String line = null;
		StringBuilder builder = new StringBuilder();
		try {
			while((line = fileData.readLine()) != null){
				builder.append(line);
			}
		} catch (IOException e) {
			model.addAttribute("error", "System has encountered some, technical issues please come back later!");
			return "machineLearningForm";
		}
		
		String readFile = builder.toString();
		Map<String,String> map = new HashMap<String,String>();
		ObjectMapper mapper = new ObjectMapper();
	 
		try {
			//convert JSON string to Map
			map = mapper.readValue(readFile,  new TypeReference<HashMap<String,String>>(){});
		} catch (Exception e) {
			model.addAttribute("error", "System has encountered some, technical issues please come back later!");
			return "machineLearningForm";
		}
		
		try{
			double carrierCO = Double.parseDouble(map.get(carrierStr));
			double aoCO = Double.parseDouble(map.get("A0"));
			double dowCO = Double.parseDouble(map.get("DAY_OF_WEEK"));
			double monthCO = Double.parseDouble(map.get("MONTH"));
			double arrDelayCO = Double.parseDouble(map.get("ARR_DELAY"));
			double timeCO = Double.parseDouble(map.get("TIME"));
			
			double summation = aoCO + carrierCO  + monthCO *month + dowCO*day+timeCO*departureTime + arrDelayCO*arrivalDelay;
			
			model.addAttribute("summation", "Predicted Delay for airport "+airportsStr+" and carrier "+carrierStr +" is: "+summation);
		}
		catch(NumberFormatException e){
			model.addAttribute("error", "System has encountered some, technical issues please come back later!");
			return "machineLearningForm";
		}
		return "machineLearningForm";
	}
	
}