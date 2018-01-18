
--6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. Using MapReduce Mode of Pig


h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
(s_no,
case_status:chararray,
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


status = FOREACH h1b GENERATE year,case_status;

--DESCRIBE status;
--status: {year: chararray,case_status: chararray}

-----------------------------------------------------------------------------------------------------------------------------------------------------


grouped = GROUP status BY year;

--DESCRIBE grouped;
--grouped: {group: chararray,status: {(year: chararray,case_status: chararray)}}


totalApp = FOREACH grouped GENERATE group as year, COUNT(status) AS totalApplication;

--DESCRIBE totalApp;
--totalApp: {year: chararray,totalApplication: long}

--DUMP totalApp;
--(2011,358767)
--(2012,415607)
--(2013,442114)
--(2014,519427)
--(2015,618727)
--(2016,647803)

------------------------------------------------------------------------------------------------------------------------------------------------------

caseStatusGroup = GROUP status BY (year,case_status);

--DESCRIBE caseStatusGroup;
--caseStatusGroup: {group: (year: chararray,case_status: chararray),status: {(year: chararray,case_status: chararray)}}


caseStatusTotal = FOREACH caseStatusGroup GENERATE group, group.year,COUNT(status) AS caseTotal;

--DESCRIBE caseStatusTotal;
--caseStatusTotal: {group: (year: chararray,case_status: chararray),year: chararray,caseTotal: long}

--DUMP caseStatusTotal;
--((2011,DENIED),2011,29130)
--((2011,CERTIFIED),2011,307936)
--((2011,WITHDRAWN),2011,10105)
--((2011,CERTIFIED-WITHDRAWN),2011,11596)
--((2012,DENIED),2012,21096)
--((2012,CERTIFIED),2012,352668)
--((2012,WITHDRAWN),2012,10725)
--((2012,CERTIFIED-WITHDRAWN),2012,31118)
--((2013,DENIED),2013,12141)
--((2013,CERTIFIED),2013,382951)
--((2013,WITHDRAWN),2013,11590)
--((2013,CERTIFIED-WITHDRAWN),2013,35432)
--((2014,DENIED),2014,11899)
--((2014,CERTIFIED),2014,455144)
--((2014,WITHDRAWN),2014,16034)
--((2014,CERTIFIED-WITHDRAWN),2014,36350)
--((2015,DENIED),2015,10923)
--((2015,CERTIFIED),2015,547278)
--((2015,WITHDRAWN),2015,19455)
--((2015,CERTIFIED-WITHDRAWN),2015,41071)
--((2016,DENIED),2016,9175)
--((2016,CERTIFIED),2016,569646)
--((2016,WITHDRAWN),2016,21890)
--((2016,CERTIFIED-WITHDRAWN),2016,47092)

---------------------------------------------------------------------------------------------------------------------------------------------------

joined = JOIN caseStatusTotal BY year, totalApp BY year;

--joined = JOIN caseStatusTotal BY $1, totalApp BY $0;

--DESCRIBE joined;
--joined: {caseStatusTotal::group: (year: chararray,case_status: chararray),caseStatusTotal::year: chararray,caseStatusTotal::caseTotal: long,totalApp::year: chararray,totalApp::totalApplication: long}



percentCountCaseStatusYear = FOREACH joined GENERATE FLATTEN($0), (FLOAT)(caseStatusTotal::caseTotal*100)/(totalApp::totalApplication) AS percentage, caseStatusTotal::caseTotal AS caseStatusCount;

--percentCountCaseStatusYear = FOREACH joined GENERATE FLATTEN($0), (FLOAT)($2*100)/$4 AS percentage, $2 AS caseStatusCount;


--DESCRIBE percentCountCaseStatusYear;
--percentCountCaseStatusYear: {caseStatusTotal::group::year: chararray,caseStatusTotal::group::case_status: chararray,percentage: float,caseStatusCount: long}


STORE percentCountCaseStatusYear INTO '/H1BVisaProject/Pig/Q6_PercentAndCountOfEachCaseStatusOnTotalApplicationForEachYear' USING PigStorage();


DUMP percentCountCaseStatusYear;

--(2011,DENIED,8.119476,29130)
--(2011,CERTIFIED,85.83175,307936)
--(2011,WITHDRAWN,2.8165913,10105)
--(2011,CERTIFIED-WITHDRAWN,3.2321813,11596)
--(2012,DENIED,5.075949,21096)
--(2012,CERTIFIED,84.856125,352668)
--(2012,WITHDRAWN,2.5805628,10725)
--(2012,CERTIFIED-WITHDRAWN,7.487362,31118)
--(2013,CERTIFIED-WITHDRAWN,8.014222,35432)
--(2013,WITHDRAWN,2.6214957,11590)
--(2013,CERTIFIED,86.61816,382951)
--(2013,DENIED,2.7461243,12141)
--(2014,CERTIFIED-WITHDRAWN,6.998096,36350)
--(2014,WITHDRAWN,3.086863,16034)
--(2014,CERTIFIED,87.624245,455144)
--(2014,DENIED,2.2907934,11899)
--(2015,DENIED,1.765399,10923)
--(2015,CERTIFIED,88.452255,547278)
--(2015,WITHDRAWN,3.1443594,19455)
--(2015,CERTIFIED-WITHDRAWN,6.6379843,41071)
--(2016,CERTIFIED,87.93507,569646)
--(2016,WITHDRAWN,3.3791137,21890)
--(2016,CERTIFIED-WITHDRAWN,7.269494,47092)
--(2016,DENIED,1.4163257,9175)


