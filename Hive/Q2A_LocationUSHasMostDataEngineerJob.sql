
--2 a) Which part of the US has the most Data Engineer jobs for each year?

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2011' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2012' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2013' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2014' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2015' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;

SELECT year,worksite, COUNT(*) AS total FROM h1b_final WHERE job_title LIKE '%DATA ENGINEER%' AND case_status = 'CERTIFIED' AND year = '2016' GROUP BY year,worksite ORDER BY total DESC LIMIT 5;


/*

output:-
year	worksite	total
2011	SEATTLE, WASHINGTON	19
2011	NEW YORK, NEW YORK	4
2011	PLANO, TEXAS	3
2011	SAN FRANCISCO, CALIFORNIA	3
2011	WALTHAM, MASSACHUSETTS	2

year	worksite	total
2012	SEATTLE, WASHINGTON	26
2012	SAN FRANCISCO, CALIFORNIA	6
2012	PONTIAC, MICHIGAN	3
2012	CUPERTINO, CALIFORNIA	2
2012	NEW YORK, NEW YORK	2

year	worksite	total
2013	SEATTLE, WASHINGTON	43
2013	SAN FRANCISCO, CALIFORNIA	16
2013	MENLO PARK, CALIFORNIA	11
2013	NEW YORK, NEW YORK	5
2013	FOSTER CITY, CALIFORNIA	3

year	worksite	total
2014	SEATTLE, WASHINGTON	42
2014	SAN FRANCISCO, CALIFORNIA	28
2014	MENLO PARK, CALIFORNIA	18
2014	NEW YORK, NEW YORK	16
2014	MOUNTAIN VIEW, CALIFORNIA	13

year	worksite	total
2015	SEATTLE, WASHINGTON	60
2015	SAN FRANCISCO, CALIFORNIA	52
2015	NEW YORK, NEW YORK	35
2015	MENLO PARK, CALIFORNIA	21
2015	MOUNTAIN VIEW, CALIFORNIA	17

year	worksite	total
2016	SEATTLE, WASHINGTON	121
2016	SAN FRANCISCO, CALIFORNIA	78
2016	NEW YORK, NEW YORK	64
2016	MENLO PARK, CALIFORNIA	38
2016	IRVINE, CALIFORNIA	17

*/

