
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


--Saving output in HDFS
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

/*
2011	CERTIFIED	INTERNATIONAL MERCHANDISE SALES MANAGER	N	4197440
2011	CERTIFIED	AUSTRALIAN OFFSHORE DRILLING REGULATION SPECIALIST	N	416000
2011	CERTIFIED	OCCUPATIONAL SAFETY AND HEALTH EXPERT	N	416000
2011	CERTIFIED	LECTURER IN ARCHITECTURE	N	405600
2011	CERTIFIED	PHYSICIAN (INTERNAL MEDICINE, HEMATOLOGY AND ONCOL	N	306342
2011	CERTIFIED-WITHDRAWN	INSTRUCTOR, CLINICAL TRACK	N	304200
2011	CERTIFIED	INTERVENTIONAL CARDIOLOGIST	N	282880
2011	CERTIFIED	ASSISTANT VISITING PROFESSOR	N	260000
2011	CERTIFIED	OBSTETRICIAN/GYNECOLOGIST	N	250348
2011	CERTIFIED	CONSULTING ORTHOPEDIC SURGEON	N	238326
2012	CERTIFIED	TEST ANALYST - US	N	92152320
2012	CERTIFIED	OCCUPATIONAL HEALTH AND SAFETY EXPERT	N	416000
2012	CERTIFIED	OCCUPATIONAL SAFETY AND HEALTH SPECIALIST	N	416000
2012	CERTIFIED	TEACHING ARTIST	N	260000
2012	CERTIFIED	PART TIME FACULTY/NON INSTRUCTIONAL	N	249995
2012	CERTIFIED	ADJUNCT ASSISTANT PROFESSOR OF HEALTH POLICY	N	243880
2012	CERTIFIED	SENIOR V.P. CHIEF BUSINESS OFFICER	N	243110
2012	CERTIFIED-WITHDRAWN	CONSULTING ORTHOPEDIC SURGEON	N	238326
2012	CERTIFIED	CEO, DEMICA	N	234353
2012	CERTIFIED	EVENING HOSPITALIST	N	225680
2013	CERTIFIED	NON-INTERVENTIONAL CARDIOLOGIST	N	392891
2013	CERTIFIED	CRITICAL CARE: INTENSIVIST	N	296254
2013	CERTIFIED-WITHDRAWN	PART-TIME FACULTY	N	293800
2013	CERTIFIED	NEURO-INTENSIVIST	N	290576
2013	CERTIFIED	PHYSICIAN (HEMATOLOGY AND ONCOLOGY)	N	251284
2013	CERTIFIED	CRITICAL CARE PHYSICIAN	N	250972
2013	CERTIFIED	ADJUNCT ASSOCIATE  PROFESSOR	N	250640
2013	CERTIFIED	ADJUNCT RECITATION LEADER	N	243880
2013	CERTIFIED	ADJUNCT ASSISTANT PROFESSOR OF HEALTH POLICY	N	243880
2013	CERTIFIED	ACADEMIC CONSULTANT ANESTHESIOLOGY	N	243464
2014	CERTIFIED	VISITING ASSOCIATE PROFESSOR	N	429520
2014	CERTIFIED	ADJUNCT ASSOCIATE MEDICAL PROFESSOR (BASIC SCIENCE	N	394825
2014	CERTIFIED	ADJUNCT ASSOCIATE  PROFESSOR	N	250640
2014	CERTIFIED	CLINICAL ASSOCIATE PROFESSOR (PSYCHIATRY)	N	250473
2014	CERTIFIED	ASSOCIATE DERMATOLOGIST	N	247603
2014	CERTIFIED-WITHDRAWN	PHYSICIAN (OB/GYN) AND PHYSICIAN PRECEPTOR	N	241904
2014	CERTIFIED-WITHDRAWN	PRESIDENT AND CEO	N	241592
2014	CERTIFIED	STAFF PHYSICIAN/SURGEON	N	240468
2014	CERTIFIED	ADJUNCT ASSISTANT MEDICAL PROFESSOR (BASIC)	N	240323
2014	CERTIFIED	INTERVENTIONAL RADIOLOGIST	N	237369
2015	CERTIFIED-WITHDRAWN	CARDIOLOGIST	N	360006
2015	CERTIFIED	VASCULAR SURGEON & SPECIALITY MEDICINE CONSULTANT	N	324688
2015	CERTIFIED	PSYCHIATRIST II	N	291200
2015	CERTIFIED	ASSISTANT PROFESSOR ADJUNCT OF ARCHITECTURE	N	284377
2015	CERTIFIED	ADJUNCT ASSOCIATE  PROFESSOR	N	257920
2015	CERTIFIED	CARDIOLOGIST	N	249454
2015	CERTIFIED	SPECIAL LEGAL COUNSEL (CORPORATE)	N	245856
2015	CERTIFIED	PHYSICIAN - EMERGENCY MEDICIN	N	242860
2015	CERTIFIED	SOCIAL ENTREPRENEUR IN RESIDENCE	N	239200
2015	CERTIFIED	PRESIDENT AND CEO	N	233272
2016	CERTIFIED	COMMERCIALIZATION SERVICES MANAGER	N	70000
2016	CERTIFIED	ASSISTANT PROFESSOR OF COMPUTER AND INFORMATION SCIENCES	N	70000
2016	CERTIFIED	CHIEF FINANACIAL OFFICER	N	70000
2016	CERTIFIED	DEMAND GENERATION MANAGER	N	70000
2016	CERTIFIED	IMMIGRATION ATTORNEY	N	70000
2016	CERTIFIED	NETWORK ARCHITECT & SECURITY SPECIALISTS	N	70000
2016	CERTIFIED	RESEARCH ENGINEER, ASSOCIATE	N	70000
2016	CERTIFIED	SOFTWARE DEVELOPER, HIGH PERFORMANCE COMPUTING APP DEVELOPER	N	70000
2016	CERTIFIED	TESTING COORDINATOR - CLINICAL PSYCHOLOGIST	N	70000
2016	CERTIFIED	VP, DEVELOPMENT AND PUBLIC AFFAIRS	N	70000
*/
