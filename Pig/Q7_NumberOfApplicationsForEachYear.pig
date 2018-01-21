
--7) Create a bar graph to depict the number of applications for each year [All]

h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
--h1b = LOAD '/home/hduser/h1b/h1b_final' USING PigStorage() AS 
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

applications = FOREACH h1b GENERATE year;

--DESCRIBE applications;
--applications: {year: chararray}


groupyear = GROUP applications BY year;

--DESCRIBE groupyear;
--groupyear: {group: chararray,applications: {(year: chararray)}}

numberofapplications = FOREACH groupyear GENERATE group AS year, COUNT(applications) AS numberapp;

--DESCRIBE numberofapplications; 
--numberofapplications: {year: chararray,numberapp: long}



--DUMP numberofapplications;
--(2011,358767)
--(2012,415607)
--(2013,442114)
--(2014,519427)
--(2015,618727)
--(2016,647803)

finalfilter = FILTER numberofapplications BY year == '$whichyear';

DUMP finalfilter;

