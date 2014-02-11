<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@page import="com.pandita.util.MapReduceUtil"%>
<%@page import="java.util.Iterator"%>
<%@page import="java.util.Map"%>
<%@ page session="false"%>
<html>
<head>
<title>Pandita: Airline Performance Per Year</title>
<link rel="stylesheet" href="resources/css/bootstrap.css"></link>
<link rel="stylesheet" href="resources/css/bootstrap-theme.css"></link>
<script type="text/javascript" src="resources/jquery.js"></script>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script>
	$(document).ready(function() {

		var $startRange = $('#startRange');
		var $endRange = $('#endRange');
		for (k = 1988; k < 2009; k++) {
			if (k != 2008)
				$startRange.append("<option value="+k+">" + k + "</option>");
			if (k != 1988)
				$endRange.append("<option value="+k+">" + k + "</option>");
		}
	});
	
	google.load("visualization", "1", {
		packages : [ "corechart" ]
	});
	google.setOnLoadCallback(drawChart);
	function drawChart() {
		
        var data = google.visualization.arrayToDataTable([
        		
        		<%
    			Map<String, Map<String, String>> data = (Map<String, Map<String, String>>)request.getAttribute("data");
    if (data != null){
    			String[] years = (String[])request.getAttribute("reqYears");
    			String[] airlines = (String[])request.getAttribute("reqAirlines");
    			StringBuilder headerBuilder = new StringBuilder("['Year', ");
    			for(int x=0; x<airlines.length; x++){
    				String airline = airlines[x];
    				headerBuilder.append("'").append(airline).append("'");
    				if (x != airlines.length-1){
    					headerBuilder.append(",");
    				}
    			}
    			out.print(headerBuilder.toString() +"], ");
    		
    			 StringBuilder bodyBuilder = new StringBuilder();
    			 for (int i=0; i<years.length; i++){
    				 String year = years[i];
    				 String totalDelayStr = data.get("AIRLINES_TOTAL_DELAY").get(year);
    				 bodyBuilder.append("['").append(year).append("',");
    				 for(int j =0; j<airlines.length; j++){
    					 	String airline = airlines[j];
    					 	if (data.get(airline) != null){
    						 	String delayStr = data.get(airline).get(year);
    						 	try{
    							 	int delay = Integer.parseInt(delayStr);
    							 	int totalDelay = Integer.parseInt(totalDelayStr);
    							 	bodyBuilder.append((100-(delay * 100/totalDelay)));
    						 	}
    							 catch(NumberFormatException e){
    								 bodyBuilder.append(-10);
    							 };
    					 } else{
    					 	bodyBuilder.append(-10);
    					 }
    					 	if (j != airlines.length-1){
						 		bodyBuilder.append(",");
						 	}
    				 }
    				 bodyBuilder.append("]");
    				 if (i != years.length-1){
    					 bodyBuilder.append(",");
    				 }
    			 }
    			out.print(bodyBuilder.toString());
    }
    %>
        ]);
		var options = {
			title : 'Company Performance'
		};

		var chart = new google.visualization.LineChart(document
				.getElementById('chart_div'));
		chart.draw(data, options);
	}
	
</script>
</head>
<body>

	<h3>${Error}</h3>

	<form method="post"
		action="${pageContext.request.contextPath}/airlinePerformace.htm"
		id="airlinePerformanceForm">
		<select multiple="multiple" name="airlines">
			<c:forEach var="airline" items="${airlines}">
				<option value="${airline}">${airline}</option>
			</c:forEach>
		</select> <select id="startRange" name="startRange"></select> <select
			id="endRange" name="endRange"></select> <input type="submit"
			value="Genereate Report" class="btn btn-default" id="submitBtn">
	</form>

	<div id="chart_div" style="width: 900px; height: 500px;"></div>
</body>
</html>
