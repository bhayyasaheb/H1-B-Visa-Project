
--Q2 b) find top 5 locations in the US who have got certified visa for each year.[certified]

USE h1b;

DROP TABLE IF EXISTS certified2011;

DROP TABLE IF EXISTS certified2012;

DROP TABLE IF EXISTS certified2013;

DROP TABLE IF EXISTS certified2014;

DROP TABLE IF EXISTS certified2015;

DROP TABLE IF EXISTS certified2016;

CREATE TABLE IF NOT EXISTS certified2011 (year string, worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2011
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2011' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;	


CREATE TABLE IF NOT EXISTS certified2012 (year string,worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2012
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2012' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


CREATE TABLE IF NOT EXISTS certified2013 (year string,worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2013
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2013' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


CREATE TABLE IF NOT EXISTS certified2014 (year string,worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2014
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2014' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


CREATE TABLE IF NOT EXISTS certified2015 (year string,worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2015
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2015' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


CREATE TABLE IF NOT EXISTS certified2016 (year string,worksite string, certifiedcount int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE certified2016
SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b_final WHERE year = '2016' AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;


--Saving output in HDFS
INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q2B_Top5LocationsInUSGotCertifiedVisaForEachYear' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
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

/*
2011	NEW YORK, NEW YORK	23172
2011	HOUSTON, TEXAS	8184
2011	CHICAGO, ILLINOIS	5188
2011	SAN JOSE, CALIFORNIA	4713
2011	SAN FRANCISCO, CALIFORNIA	4711
2012	NEW YORK, NEW YORK	23737
2012	HOUSTON, TEXAS	9963
2012	SAN FRANCISCO, CALIFORNIA	6116
2012	CHICAGO, ILLINOIS	5671
2012	ATLANTA, GEORGIA	5565
2013	NEW YORK, NEW YORK	23537
2013	HOUSTON, TEXAS	11136
2013	SAN FRANCISCO, CALIFORNIA	7281
2013	SAN JOSE, CALIFORNIA	6722
2013	ATLANTA, GEORGIA	6377
2014	NEW YORK, NEW YORK	27634
2014	HOUSTON, TEXAS	13360
2014	SAN FRANCISCO, CALIFORNIA	9798
2014	SAN JOSE, CALIFORNIA	8223
2014	ATLANTA, GEORGIA	8213
2015	NEW YORK, NEW YORK	31266
2015	HOUSTON, TEXAS	15242
2015	SAN FRANCISCO, CALIFORNIA	12594
2015	ATLANTA, GEORGIA	10500
2015	SAN JOSE, CALIFORNIA	9589
2016	NEW YORK, NEW YORK	34639
2016	SAN FRANCISCO, CALIFORNIA	13836
2016	HOUSTON, TEXAS	13655
2016	ATLANTA, GEORGIA	11678
2016	CHICAGO, ILLINOIS	11064
*/


