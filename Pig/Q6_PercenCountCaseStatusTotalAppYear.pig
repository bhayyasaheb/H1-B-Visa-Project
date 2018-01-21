
--6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.


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

status = FOREACH h1b GENERATE year,case_status;

--DESCRIBE status;
--status: {year: chararray,case_status: chararray}

----------------------------------------------------------------------------------------------------------------------------

grouped = GROUP status BY year;

--DESCRIBE grouped;
--grouped: {group: chararray,status: {(year: chararray,case_status: chararray)}}

total = FOREACH grouped GENERATE group AS year, COUNT(status.case_status) AS totalApplication;

--DESCRIBE total;
--total: {year: chararray,totalApplication: long}

--DUMP total
--(2011,358767)
--(2012,415607)
--(2013,442114)
--(2014,519427)
--(2015,618727)
--(2016,647803)


newgroup = GROUP status by (year,case_status);

--DESCRIBE newgroup;
--newgroup: {group: (year: chararray,case_status: chararray),status: {(year: chararray,case_status: chararray)}}

casestatuscount = FOREACH newgroup GENERATE group,group.year,COUNT(status) AS caseTotal;

--DESCRIBE casestatuscount;
--casestatuscount: {group: (year: chararray,case_status: chararray),year: chararray,caseTotal: long}

joined = JOIN casestatuscount BY $1, total BY $0;

--DESCRIBE joined;
--joined: {casestatuscount::group: (year: chararray,case_status: chararray),casestatuscount::year: chararray,casestatuscount::caseTotal: long,total::year: chararray,total::totalApplication: long}

final = FOREACH joined GENERATE FLATTEN($0), (float)($2*100)/$4 AS percentage, $2 AS caseStatusCount;

--DESCRIBE final;
--final: {casestatuscount::group::year: chararray,casestatuscount::group::case_status: chararray,percentage: float,caseStatusCount: long}

--DUMP final;
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


filterfinal = FILTER final BY $0 == '$whichyear';

DUMP filterfinal;

