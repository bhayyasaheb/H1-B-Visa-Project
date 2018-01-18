
--Q1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

USE h1b;

DROP TABLE IF EXISTS jobtitle;

DROP TABLE IF EXISTS jobtitlepercent1;

CREATE TABLE IF NOT EXISTS jobtitle(year string, job_title string, total int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE jobtitle SELECT year,job_title,COUNT(job_title) AS total FROM h1b_final GROUP BY year,job_title ORDER BY year,job_title;


CREATE TABLE IF NOT EXISTS jobtitlepercent1(job_title string, growthpercent float)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE jobtitlepercent1 SELECT job_title, ROUND(((total-LAG(total)OVER(ORDER BY year))*100)/LAG(total)OVER(ORDER BY year),2) AS growthpercent FROM jobtitle;


--Saving output in HDFS
INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q1B_Top5JobTitleHighAvgGrowth' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT job_title, ROUND(AVG(growthpercent),2) AS avggrowth FROM jobtitlepercent1 GROUP BY job_title ORDER BY avggrowth DESC LIMIT 5;


SELECT job_title, ROUND(AVG(growthpercent),2) AS avggrowth FROM jobtitlepercent1 GROUP BY job_title ORDER BY avggrowth DESC LIMIT 5;
--PROGRAMMER ANALYST	2672582.23
--COMPUTER PROGRAMMER	1176066.67
--SOFTWARE ENGINEER	1124539.17
--SOFTWARE DEVELOPER	715016.67
--SYSTEMS ANALYST	698058.33

