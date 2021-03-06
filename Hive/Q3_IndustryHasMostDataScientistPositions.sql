
--3)Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]

USE h1b;


--Saving output in HDFS
INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q3_IndustryHasMostDataScientistPositions' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
SELECT soc_name, COUNT(soc_name) AS total FROM h1b_final WHERE case_status = 'CERTIFIED' AND job_title LIKE '%DATA SCIENTIST%' GROUP BY soc_name ORDER BY total DESC LIMIT 5;


SELECT soc_name, COUNT(soc_name) AS total FROM h1b_final WHERE case_status = 'CERTIFIED' AND job_title LIKE '%DATA SCIENTIST%' GROUP BY soc_name ORDER BY total DESC LIMIT 5;

--soc_name	total
--STATISTICIANS	572
--COMPUTER AND INFORMATION RESEARCH SCIENTISTS	419
--OPERATIONS RESEARCH ANALYSTS	380
--Computer and Information Research Scientists	181
--COMPUTER OCCUPATIONS, ALL OTHER	160

