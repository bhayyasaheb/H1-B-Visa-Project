
--6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.

USE h1b;

DROP TABLE IF EXISTS total_applications;


CREATE TABLE IF NOT EXISTS total_applications (year string, total int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE total_applications SELECT year,COUNT(*) FROM h1b_final GROUP BY year;

--Saving output in HDFS
INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q6_PercentCountCaseStatusYear' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
SELECT  a.year, a.case_status,COUNT(*) AS statusTotal, ROUND((COUNT(*)/b.total)*100, 2) AS finalPercent FROM h1b_final a, total_applications b WHERE a.year = b.year GROUP BY a.case_status,b.total,a.year ORDER BY a.year,a.case_status;


SELECT  a.year, a.case_status,COUNT(*) AS statusTotal, ROUND((COUNT(*)/b.total)*100, 2) AS finalPercent FROM h1b_final a, total_applications b WHERE a.year = b.year GROUP BY a.case_status,b.total,a.year ORDER BY a.year,a.case_status;

--a.year	a.case_status	statustotal	finalpercent
--2011	CERTIFIED	307936	85.83
--2011	CERTIFIED-WITHDRAWN	11596	3.23
--2011	DENIED	29130	8.12
--2011	WITHDRAWN	10105	2.82
--2012	CERTIFIED	352668	84.86
--2012	CERTIFIED-WITHDRAWN	31118	7.49
--2012	DENIED	21096	5.08
--2012	WITHDRAWN	10725	2.58
--2013	CERTIFIED	382951	86.62
--2013	CERTIFIED-WITHDRAWN	35432	8.01
--2013	DENIED	12141	2.75
--2013	WITHDRAWN	11590	2.62
--2014	CERTIFIED	455144	87.62
--2014	CERTIFIED-WITHDRAWN	36350	7.0
--2014	DENIED	11899	2.29
--2014	WITHDRAWN	16034	3.09
--2015	CERTIFIED	547278	88.45
--2015	CERTIFIED-WITHDRAWN	41071	6.64
--2015	DENIED	10923	1.77
--2015	WITHDRAWN	19455	3.14
--2016	CERTIFIED	569646	87.94
--2016	CERTIFIED-WITHDRAWN	47092	7.27
--2016	DENIED	9175	1.42
--2016	WITHDRAWN	21890	3.38

