<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Pandita : Prediction Engine</title>
<link rel="stylesheet" href="resources/css/bootstrap.css"></link>
<link rel="stylesheet" href="resources/css/bootstrap-theme.css"></link>
<script type="text/javascript" src="resources/jquery.js"></script>
<script type="text/javascript" src="https://www.google.com/jsapi"></script>
<script>

$(document).ready(function() {

	var $day = $('#day');
	for (k = 1; k < 8; k++) {
			$day.append("<option value="+k+">" + k + "</option>");
	}
	var $month = $('#month');
	for (j = 1; j < 13; j++) {
			$month.append("<option value="+j+">" + j + "</option>");
	}
	
	var $carriers = $('#carriers');
	var carriersArr = ['AA', 'TW', 'HP', 'US', 'DL', 'NW', 'UA', 'CO', 'WN', 'FL'];
	for (i = 1; i < carriersArr.length; i++) {
		$carriers.append("<option value="+carriersArr[i]+">" + carriersArr[i] + "</option>");
	}
	
	var $airports = $('#airports');
	var airportsArr = ['ATL', 'ORD', 'LAX', 'MIA', 'DFW', 'DEN', 'JFK', 'SFO', 'CLT', 'LAS'];
	for (j = 1; j < airportsArr.length; j++) {
		$airports.append("<option value="+airportsArr[j]+">" + airportsArr[j] + "</option>");
	}
	
});

</script>
</head>
<body>
<div class="container">

<h3>${error }</h3>

<form method="get" action="${pageContext.request.contextPath}/predictionEngineCompute.htm">
<p>
	 Day: <select id="day" name="day"></select>
			  Month: <select id="month" name="month"></select>
</p>

<p>Departure Time: <input type="text" name="departureTime"></p>
<p>Carriers: <select id="carriers" name="carrier"></select></p>
<p>Airports: <select id="airports" name="airports"></select></p>

<p>Arrival Delay (In mins) <input type="text" name="arrivalDelay"/></p>
<p><input type="submit" value="Predict" class="btn btn-primary"/></p>
</form>

<h2 style="color: red">${summation}</h2>

</div>
</body>
</html>