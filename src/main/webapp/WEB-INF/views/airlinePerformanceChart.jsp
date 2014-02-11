<%@page import="java.util.Iterator"%>
<%@page import="java.util.Map"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<html>
<head>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script type="text/javascript">
	google.load("visualization", "1", {
		packages : [ "corechart" ]
	});
	google.setOnLoadCallback(drawChart);
	function drawChart() {
		var data = google.visualization
				.arrayToDataTable([
			<%
				Map<String, Map<String, String>> data = (Map<String, Map<String, String>>)request.getAttribute("data");
				Iterator<String> airlines = data.keySet().iterator();
				String airline;
				StringBuilder headerBuilder = new StringBuilder("'[Year', ");
				while(airlines.hasNext()){
				airline = airlines.next();
				headerBuilder.append("'"+airline+"', ");
				}
				out.print(headerBuilder.toString() +"]]);");
			%>
							

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
	<div id="chart_div" style="width: 900px; height: 500px;"></div>
</body>

</html>