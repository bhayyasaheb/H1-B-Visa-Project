

--8) Find the average Prevailing Wage for each Job for each Year for part time job position. Arrange the output in descending order - [Certified and Certified Withdrawn.]


--h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
(s_no,case_status:chararray,
employer_name:chararray,
soc_name:chararray,
job_title:chararray,
full_time_position:chararray,
prevailing_wage:long,
year:chararray,
worksite:chararray,
longitute:double,
latitute:double);

--DESCRIBE h1b;
--h1b: {s_no: bytearray,case_status: chararray,employer_name: chararray,soc_name: chararray,job_title: chararray,full_time_position: chararray,prevailing_wage: long,year: chararray,worksite: chararray,longitute: double,latitute: double}


job = FOREACH h1b GENERATE year,case_status,full_time_position,prevailing_wage,job_title;

--DESCRIBE job;
--job: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}


finalh1b = FILTER job BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

--DESCRIBE finalh1b;                                                                          
--finalh1b: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}

parttime = FILTER finalh1b BY full_time_position == 'N';

--DESCRIBE fulltime;                                      
--fulltime: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}

------------------------------------------------------------------------------------------------------------------------------------------

filter2011 = FILTER parttime BY year == '2011';

--DESCRIBE filter2011;                           
--filter2011: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}


group2011 = GROUP filter2011 BY (year,job_title);

--DESCRIBE group2011;                              
--group2011: {group: (year: chararray,job_title: chararray),filter2011: {(year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray)}}


avg2011 = FOREACH group2011 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2011.prevailing_wage) AS avgprewage;

--DESCRIBE avg2011;
--avg2011: {year: chararray,job_title: chararray,avgprewage: double}


avgprewage2011 = LIMIT (ORDER avg2011 BY $2 DESC) 5;

---------------------------------------------------------------------------------------------------------------------------------------

filter2012 = FILTER parttime BY year == '2012';

group2012 = GROUP filter2012 BY (year,job_title);

avg2012 = FOREACH group2012 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2012.prevailing_wage) AS avgprewage;

avgprewage2012 = LIMIT (ORDER avg2012 BY $2 DESC) 5;


filter2013 = FILTER parttime BY year == '2013';

group2013 = GROUP filter2013 BY (year,job_title);

avg2013 = FOREACH group2013 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2013.prevailing_wage) AS avgprewage;

avgprewage2013 = LIMIT (ORDER avg2013 BY $2 DESC) 5;


filter2014 = FILTER parttime BY year == '2014';

group2014 = GROUP filter2014 BY (year,job_title);

avg2014 = FOREACH group2014 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2014.prevailing_wage) AS avgprewage;

avgprewage2014 = LIMIT (ORDER avg2014 BY $2 DESC) 5;


filter2015 = FILTER parttime BY year == '2015';

group2015 = GROUP filter2015 BY (year,job_title);

avg2015 = FOREACH group2015 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2015.prevailing_wage) AS avgprewage;

avgprewage2015 = LIMIT (ORDER avg2015 BY $2 DESC) 5;


filter2016 = FILTER parttime BY year == '2016';

group2016 = GROUP filter2016 BY (year,job_title);

avg2016 = FOREACH group2016 GENERATE FLATTEN(group) AS (year,job_title), (double)AVG(filter2016.prevailing_wage) AS avgprewage;

avgprewage2016 = LIMIT (ORDER avg2016 BY $2 DESC) 5;

-------------------------------------------------------------------------------------------------------------------------------------------------

AvgPreWageJobYearPartTime = UNION avgprewage2011, avgprewage2012, avgprewage2013, avgprewage2014, avgprewage2015, avgprewage2016;


--DESCRIBE AvgPreWageJobYearPartTime;
--AvgPreWageJobYearPartTime: {year: chararray,job_title: chararray,avgprewage: double}

DUMP AvgPreWageJobYearPartTime;
--(2011,INTERNATIONAL MERCHANDISE SALES MANAGER,4197440.0)
--(2011,AUSTRALIAN OFFSHORE DRILLING REGULATION SPECIALIST,416000.0)
--(2011,OCCUPATIONAL SAFETY AND HEALTH EXPERT,416000.0)
--(2011,LECTURER IN ARCHITECTURE,405600.0)
--(2011,PHYSICIAN (INTERNAL MEDICINE, HEMATOLOGY AND ONCOL,306342.0)
--(2016,IMMIGRATION ATTORNEY,70000.0)
--(2016,NETWORK ARCHITECT & SECURITY SPECIALISTS,70000.0)
--(2016,RESEARCH ENGINEER, ASSOCIATE,70000.0)
--(2016,DEMAND GENERATION MANAGER,70000.0)
--(2016,SOFTWARE DEVELOPER, HIGH PERFORMANCE COMPUTING APP DEVELOPER,70000.0)
--(2014,VISITING ASSOCIATE PROFESSOR,429520.0)
--(2014,ADJUNCT ASSOCIATE MEDICAL PROFESSOR (BASIC SCIENCE,394825.0)
--(2014,ADJUNCT ASSOCIATE  PROFESSOR,250640.0)
--(2014,CLINICAL ASSOCIATE PROFESSOR (PSYCHIATRY),250473.0)
--(2014,ASSOCIATE DERMATOLOGIST,247603.0)
--(2015,VASCULAR SURGEON & SPECIALITY MEDICINE CONSULTANT,324688.0)
--(2015,CARDIOLOGIST,304730.0)
--(2015,PSYCHIATRIST II,291200.0)
--(2015,ASSISTANT PROFESSOR ADJUNCT OF ARCHITECTURE,284377.0)
--(2015,ADJUNCT ASSOCIATE  PROFESSOR,257920.0)
--(2012,TEST ANALYST - US,9.215232E7)
--(2012,OCCUPATIONAL HEALTH AND SAFETY EXPERT,416000.0)
--(2012,OCCUPATIONAL SAFETY AND HEALTH SPECIALIST,416000.0)
--(2012,TEACHING ARTIST,260000.0)
--(2012,PART TIME FACULTY/NON INSTRUCTIONAL,249995.0)
--(2013,NON-INTERVENTIONAL CARDIOLOGIST,392891.0)
--(2013,CRITICAL CARE: INTENSIVIST,296254.0)
--(2013,NEURO-INTENSIVIST,290576.0)
--(2013,PHYSICIAN (HEMATOLOGY AND ONCOLOGY),251284.0)
--(2013,CRITICAL CARE PHYSICIAN,250972.0)




