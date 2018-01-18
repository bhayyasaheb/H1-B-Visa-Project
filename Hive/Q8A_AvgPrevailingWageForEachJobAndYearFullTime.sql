
--8) Find the average Prevailing Wage for each Job for each Year For only job positions full time. Arrange the output in descending order - [Certified and Certified Withdrawn.]

USE h1b;

DROP TABLE IF EXISTS avgprewagefull2011;

DROP TABLE IF EXISTS avgprewagefull2012;

DROP TABLE IF EXISTS avgprewagefull2013;

DROP TABLE IF EXISTS avgprewagefull2014;

DROP TABLE IF EXISTS avgprewagefull2015;

DROP TABLE IF EXISTS avgprewagefull2016;

CREATE TABLE IF NOT EXISTS avgprewagefull2011 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2011
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2011' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;

CREATE TABLE IF NOT EXISTS avgprewagefull2012 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2012
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2012' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagefull2013 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2013
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2013' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagefull2014 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2014
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2014' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;

CREATE TABLE IF NOT EXISTS avgprewagefull2015 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2015
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2015' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;


CREATE TABLE IF NOT EXISTS avgprewagefull2016 (year string, case_status string, job_title string, full_time_position string, avgprewage bigint)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE avgprewagefull2016
SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b_final WHERE year = '2016' AND full_time_position = 'Y'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC LIMIT 10;



INSERT OVERWRITE DIRECTORY '/H1BVisaProject/Hive/Q8A_Top10AvgPreWageForEachJobAndYearFullTime' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
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

/*
2011	CERTIFIED-WITHDRAWN	AREA MANAGER, PHARMACEUTICAL PACKAGING	Y	212987840
2011	CERTIFIED-WITHDRAWN	DEVELOPER (SOFTWARE SYSTEMS APPLICATIONS)	Y	176560800
2011	CERTIFIED	SYSTEMS ENGINEER (DIAGNOSTICS)	Y	95526080
2011	CERTIFIED	SENIOR COST CONSULTANT	Y	85619040
2011	CERTIFIED	RADIATION ONCOLOGIST	Y	76602640
2011	CERTIFIED	IT ADMINSTRATIVE ASSISTANT	Y	71114118
2011	CERTIFIED	VERIFICATION AND VALIDATION ENGINEER	Y	66867819
2011	CERTIFIED	SENIOR STAFF TEACHER	Y	33221084
2011	CERTIFIED	TECHNICAL SOFTWARE CONSULTANT	Y	16386115
2011	CERTIFIED	PRINCIPAL ARCHITECT	Y	9925884
2012	CERTIFIED	QA COORDINATOR	Y	23678542
2012	CERTIFIED	SAS PROGRAMMER	Y	1310280
2012	CERTIFIED	LECTURER IN HORTICULTURE	Y	693360
2012	CERTIFIED	CARDIOLOGIST / ELECTROPHYSIOLOGIST	Y	475780
2012	CERTIFIED	TEACHER, DEAF & HARD OF HEARING	Y	473773
2012	CERTIFIED	ORTHOPAEDIC TRAUMA AND HAND SURGEON, RECONSTRUCTIV	Y	416082
2012	CERTIFIED	INTERIM CEO	Y	378861
2012	CERTIFIED-WITHDRAWN	PHYSICIAN (CARDIOLOGIST)	Y	353689
2012	CERTIFIED	ASST. PROFESSOR/RADIOLOGIST	Y	327600
2012	CERTIFIED	EXECUTIVE VICE-PRESIDENT, STRATEGIC ACCOUNTS	Y	302387
2013	CERTIFIED	STAFF CONSULTANT - MICRO	Y	169507520
2013	CERTIFIED	QA ANALYST/ PROGRAMMER	Y	70069927
2013	CERTIFIED-WITHDRAWN	SOFTWARE QUALITY ASSURANCE ENGINEER	Y	2168876
2013	CERTIFIED	SOFTWARE PROJ. MGR./ARCHITECT	Y	891072
2013	CERTIFIED	CLINICAL FELLOW, MINIMALLY INVASIVE SURGERY	Y	590913
2013	CERTIFIED	PEDIATRIC NEUROSURGEON	Y	401472
2013	CERTIFIED	NON-CARDIAC THORACIC SURGEON	Y	372175
2013	CERTIFIED	PEDIATRIC SURGEON	Y	360141
2013	CERTIFIED	PHYSICIAN (ONCOLOGIST)	Y	341758
2013	CERTIFIED	PHYSICIAN (ONCOLOGY)	Y	341758
2014	CERTIFIED	GASTROENTEROLOGIST PHYSICIAN	Y	631920
2014	CERTIFIED	PHYSICIAN/NEUROSURGEON	Y	523713
2014	CERTIFIED	MEDICAL ONCOLOGY PHYSICIAN	Y	483052
2014	CERTIFIED-WITHDRAWN	GENERAL LEDGER ACCOUNTANT	Y	481476
2014	CERTIFIED	PHYSICIAN CARDIOLOGIST	Y	467771
2014	CERTIFIED	PHYSICIAN (CARDIOLOGY/ CARDIAC ELECTROPHYSIOLOGY)	Y	448100
2014	CERTIFIED	PHYSICIAN, BARIATRIC SURGEON	Y	402930
2014	CERTIFIED	ORTHOPEDIC SURGEON AND SPORTS MEDICINE PHYSICIAN	Y	402584
2014	CERTIFIED	PHYSICIAN AND SCIENTIST (THORACIC AND REGENERATIVE	Y	377000
2014	CERTIFIED	PHYSICIAN - INTERVENTIONAL CARDIOLOGY	Y	374300
2015	CERTIFIED	MANAGER, GEORGIAN, CAUCASUS, AND EASTERN EUROPE REGIONAL MAN	Y	123086080
2015	CERTIFIED	ENGINEERING QUALITY ANALYST (15-1199.0)	Y	99939840
2015	CERTIFIED	SR. MANAGER, SOX & INTERNAL AUDIT GROUP	Y	90659805
2015	CERTIFIED	CHIEF EXECUTIVE OFFICER (CEO)	Y	453870
2015	CERTIFIED	PHYSICIAN (GENERAL SURGERY)	Y	356900
2015	CERTIFIED	INTENSIVIST (PULMONARY AND CRITICAL CARE PHYSICIAN)	Y	355634
2015	CERTIFIED	PHYSICIAN/ORTHOPEDIC SURGEON	Y	349900
2015	CERTIFIED	SURGEON AND FACULTY MEMBER	Y	340058
2015	CERTIFIED	ANESTHESIOLOGIST MD	Y	336000
2015	CERTIFIED	PHYSICIAN, UROLOGIST	Y	330166
2016	CERTIFIED	SYSTEMS ANALYSTS	Y	4869855
2016	CERTIFIED	CARDIOLOGIST/INTERVENTIONAL CARDIOLOGIST	Y	350000
2016	CERTIFIED	CARDIOLOGIST PHYSICIAN	Y	337800
2016	CERTIFIED-WITHDRAWN	TRAUMA & GENERAL SURGEON	Y	328972
2016	CERTIFIED-WITHDRAWN	ORTHOPEDIC SURGEON	Y	327529
2016	CERTIFIED	MEDICAL ONCOLOGIST AND MEDICAL DIRECTOR	Y	292138
2016	CERTIFIED	ORTHOPEDIC SPINE SURGEON	Y	290181
2016	CERTIFIED	EXECUTIVE VICE PRESIDENT, CORPORATE DEVELOPMENT	Y	289640
2016	CERTIFIED	EXECUTIVE VP - GLOBAL BUSINESS DEVELOPMENT & STRATEGY	Y	283500
2016	CERTIFIED	STAFF PHYSICIAN (SURGICAL ONCOLOGY)	Y	280000
*/

