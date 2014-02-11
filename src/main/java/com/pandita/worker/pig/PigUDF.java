package com.pandita.worker.pig;


/**
 * @author blackhat
 * 
 */
public class PigUDF {
//	extends EvalFunc<String> {
//}
//
//	private MapReduceUtil mapReduceUtil  = new MapReduceUtil();
//	
//	public String exec(Tuple input) throws IOException {
//		if (input == null || input.size() == 0){
//			return null;
//		}
//		
//		String monthStr = (String) input.get(0);
//		
//		int month;
//		try{
//			month = Integer.parseInt(monthStr);
//		} catch(NumberFormatException e){
//			e.printStackTrace();
//			return null;
//		}
//		
//		String season = null;
//		if (month > 2 && month < 6){
//			season = "SPRING";
//		}
//		else if (month > 5 && month < 9){
//			season = "SUMMER";
//		}
//		else if (month > 8 && month < 12){
//			season = "FALL";
//		}
//		else if (month == 12 || (month < 3 && month > 0)){
//			season = "WINTER";
//		}
//		
//		return season;
//	}

}
