
--8) Find the average Prevailing Wage for each Job for each Year For only job positions part time. Arrange the output in descending order - [Certified and Certified Withdrawn.]


USE h1b;

DROP TABLE IF EXISTS avgprewagepart2011;

DROP TABLE IF EXISTS avgprewagepart2012;

DROP TABLE IF EXISTS avgprewagepart2013;

DROP TABLE IF EXISTS avgprewagepart2014;

DROP TABLE IF EXISTS avgprewagepart2015;

DROP TABLE IF EXISTS avgprewagepart2016;

CREATE TABLE IF NOT EXISTS avgprewagepart2011 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2011
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2011' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagepart2012 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2012
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2012' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagepart2013 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2013
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2013' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagepart2014 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2014
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2014' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagepart2015 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2015
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2015' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;

CREATE TABLE IF NOT EXISTS avgprewagepart2016 (year string, case_status string, job_title string, full_time_position string,avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagepart2016
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2016' AND full_time_position = 'N' AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q8B_Top10AvgPreWageForEachJobAndYearPartTime' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
SELECT * FROM(
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2011
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2012
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2013
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2014
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2015
UNION
SELECT year, case_status, job_title, full_time_position, avgprewage  FROM avgprewagepart2016
) Top10AvgPreWageForEachJobAndYearPartTime
ORDER BY year,avgprewage DESC; 


