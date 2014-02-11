<%-- <%@ page language="java" contentType="text/html; charset=UTF-8" --%>
<%-- 	pageEncoding="UTF-8"%> --%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<html>
<head>
<title>Pandita: Busy and Delay By Airport</title>
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

		var data = google.visualization.arrayToDataTable([]);
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
		action="${pageContext.request.contextPath}/busyAndDelayByAirportPerYear.htm">
		<select multiple="multiple" name="airlines">
			<c:forEach var="airport" items="${airports}">
				<option value="${airport}">${airport}</option>
			</c:forEach>
		</select> <select id="startRange" name="startRange"></select> <select
			id="endRange" name="endRange"></select> <input type="submit"
			value="Genereate Report" class="btn btn-default" id="submitBtn">
	</form>

	<div id="chart_div" style="width: 900px; height: 500px;"></div>
</body>
</html>