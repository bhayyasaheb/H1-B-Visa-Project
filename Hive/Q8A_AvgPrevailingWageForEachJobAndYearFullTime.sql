
USE h1b;

--CREATE TABLE avgprewagefull2011 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2011
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2011' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;

--CREATE TABLE avgprewagefull2012 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2012
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2012' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


--CREATE TABLE avgprewagefull2013 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2013
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2013' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


--CREATE TABLE avgprewagefull2014 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2014
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2014' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;

--CREATE TABLE avgprewagefull2015 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2015
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2015' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


--CREATE TABLE avgprewagefull2016 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
--ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE avgprewagefull2016
--SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2016' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;



--INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q8A_Top10AvgPreWageForEachJobAndYearFullTime' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
SELECT * FROM(
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2011
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2012
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2013
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2014
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2015
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagefull2016
) Top10AvgPreWageForEachJobAndYearFullTime
ORDER BY year,avgprewage DESC; 


