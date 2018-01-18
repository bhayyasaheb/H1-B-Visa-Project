
--Q2 b) find top 5 locations in the US who have got certified visa for each year.[certified]

USE h1b;

--CREATE TABLE certified2011 (year string, worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2011
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2011' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;	


--CREATE TABLE certified2012 (year string,worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2012
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2012' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


--CREATE TABLE certified2013 (year string,worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2013
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2013' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


--CREATE TABLE certified2014 (year string,worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2014
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2014' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


--CREATE TABLE certified2015 (year string,worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2015
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2015' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


--CREATE TABLE certified2016 (year string,worksite string, certifiedcount int)
--ROW FORMAT DELIMITED
--FIELDS TERMINATED BY '\t';

--INSERT OVERWRITE TABLE certified2016
--SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2016' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;

--INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q2B_Top5LocationsInUSGotCertifiedVisaForEachYear' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
SELECT * FROM(
SELECT year,worksite,certifiedcount FROM certified2011
UNION
SELECT year,worksite,certifiedcount FROM certified2012
UNION
SELECT year,worksite,certifiedcount FROM certified2013
UNION
SELECT year,worksite,certifiedcount FROM certified2014
UNION
SELECT year,worksite,certifiedcount FROM certified2015
UNION
SELECT year,worksite,certifiedcount FROM certified2016
) top5USLocationsGotCertifiedVisaForEachYear
ORDER BY year,certifiedcount DESC; 


