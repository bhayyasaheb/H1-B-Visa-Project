
--8) Find the average Prevailing Wage for each Job for each Year for full time job position. Arrange the output in descending order - [Certified and Certified Withdrawn.]


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


fulltime = FILTER finalh1b BY full_time_position == 'Y';

--DESCRIBE fulltime;                                      
--fulltime: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}

---------------------------------------------------------------------------------------------------------------------------------------

filter2011 = FILTER fulltime BY year == '2011';

--DESCRIBE filter2011;                           
--filter2011: {year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray}


group2011 = GROUP filter2011 BY (year,job_title);

--DESCRIBE group2011;                              
--group2011: {group: (year: chararray,job_title: chararray),filter2011: {(year: chararray,case_status: chararray,full_time_position: chararray,prevailing_wage: long,job_title: chararray)}}


avg2011 = FOREACH group2011 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2011.prevailing_wage) AS avgprewage;

--DESCRIBE avg2011;
--avg2011: {year: chararray,job_title: chararray,avgprewage: double}


avgprewage2011 = LIMIT (ORDER avg2011 BY $2 DESC) 5;

--DUMP avgprewage2011;
--(2011,AREA MANAGER, PHARMACEUTICAL PACKAGING,2.1298784E8)
--(2011,DEVELOPER (SOFTWARE SYSTEMS APPLICATIONS),1.765608E8)
--(2011,SYSTEMS ENGINEER (DIAGNOSTICS),9.552608E7)
--(2011,SENIOR COST CONSULTANT,8.561904E7)
--(2011,RADIATION ONCOLOGIST,7.660264025E7)

----------------------------------------------------------------------------------------------------------------------------------------

filter2012 = FILTER fulltime BY year == '2012';

group2012 = GROUP filter2012 BY (year,job_title);

avg2012 = FOREACH group2012 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2012.prevailing_wage) AS avgprewage;

avgprewage2012 = LIMIT (ORDER avg2012 BY $2 DESC) 5;


filter2013 = FILTER fulltime BY year == '2013';

group2013 = GROUP filter2013 BY (year,job_title);

avg2013 = FOREACH group2013 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2013.prevailing_wage) AS avgprewage;

avgprewage2013 = LIMIT (ORDER avg2013 BY $2 DESC) 5;


filter2014 = FILTER fulltime BY year == '2014';

group2014 = GROUP filter2014 BY (year,job_title);

avg2014 = FOREACH group2014 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2014.prevailing_wage) AS avgprewage;

avgprewage2014 = LIMIT (ORDER avg2014 BY $2 DESC) 5;


filter2015 = FILTER fulltime BY year == '2015';

group2015 = GROUP filter2015 BY (year,job_title);

avg2015 = FOREACH group2015 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2015.prevailing_wage) AS avgprewage;

avgprewage2015 = LIMIT (ORDER avg2015 BY $2 DESC) 5;


filter2016 = FILTER fulltime BY year == '2016';

group2016 = GROUP filter2016 BY (year,job_title);

avg2016 = FOREACH group2016 GENERATE FLATTEN(group) AS (year,job_title), AVG(filter2016.prevailing_wage) AS avgprewage;

avgprewage2016 = LIMIT (ORDER avg2016 BY $2 DESC) 5;

--------------------------------------------------------------------------------------------------------------------------------------------


AvgPreWageJobYearFullTime = UNION avgprewage2011, avgprewage2012, avgprewage2013, avgprewage2014, avgprewage2015, avgprewage2016;


--DESCRIBE AvgPreWageJobYearFullTime;
--AvgPreWageJobYearFullTime: {year: chararray,job_title: chararray,avgprewage: double}


DUMP AvgPreWageJobYearFullTime;

--(2015,MANAGER, GEORGIAN, CAUCASUS, AND EASTERN EUROPE REGIONAL MAN,1.2308608E8)
--(2015,ENGINEERING QUALITY ANALYST (15-1199.0),9.993984E7)
--(2015,SR. MANAGER, SOX & INTERNAL AUDIT GROUP,9.06598055E7)
--(2015,CHIEF EXECUTIVE OFFICER (CEO),453870.5)
--(2015,PHYSICIAN (GENERAL SURGERY),356900.0)
--(2011,AREA MANAGER, PHARMACEUTICAL PACKAGING,2.1298784E8)
--(2011,DEVELOPER (SOFTWARE SYSTEMS APPLICATIONS),1.765608E8)
--(2011,SYSTEMS ENGINEER (DIAGNOSTICS),9.552608E7)
--(2011,SENIOR COST CONSULTANT,8.561904E7)
--(2011,RADIATION ONCOLOGIST,7.660264025E7)
--(2014,GASTROENTEROLOGIST PHYSICIAN,631920.0)
--(2014,PHYSICIAN/NEUROSURGEON,523713.0)
--(2014,MEDICAL ONCOLOGY PHYSICIAN,483052.0)
--(2014,PHYSICIAN CARDIOLOGIST,467771.0)
--(2014,PHYSICIAN (CARDIOLOGY/ CARDIAC ELECTROPHYSIOLOGY),448100.0)
--(2016,SYSTEMS ANALYSTS,4216025.545454546)
--(2016,CARDIOLOGIST/INTERVENTIONAL CARDIOLOGIST,350000.0)
--(2016,CARDIOLOGIST PHYSICIAN,337800.0)
--(2016,TRAUMA & GENERAL SURGEON,328972.0)
--(2016,MEDICAL ONCOLOGIST AND MEDICAL DIRECTOR,292138.0)
--(2013,STAFF CONSULTANT - MICRO,1.6950752E8)
--(2013,QA ANALYST/ PROGRAMMER,4.6735804E7)
--(2013,SOFTWARE PROJ. MGR./ARCHITECT,891072.0)
--(2013,CLINICAL FELLOW, MINIMALLY INVASIVE SURGERY,590913.0)
--(2013,PEDIATRIC NEUROSURGEON,401472.0)
--(2012,QA COORDINATOR,2.36785424E7)
--(2012,SAS PROGRAMMER,1253141.9454545456)
--(2012,LECTURER IN HORTICULTURE,693360.0)
--(2012,CARDIOLOGIST / ELECTROPHYSIOLOGIST,475780.0)
--(2012,TEACHER, DEAF & HARD OF HEARING,473773.0)





