
h1b = LOAD '/user/hive/warehouse/h1b.db/h1b_final' USING PigStorage() AS 
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

finalh1b = FOREACH h1b GENERATE case_status,job_title;

--DESCRIBE finalh1b;
--finalh1b: {case_status: chararray,job_title: chararray}

-------------------------------------------------------------------------------------------------------------------------------------

allgrouped = GROUP finalh1b BY job_title;

--DESCRIBE allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,job_title: chararray)}}


allcount = FOREACH allgrouped GENERATE group as job_title,COUNT(finalh1b) AS totalapplicaion;

--DESCRIBE allcount;
--allcount: {job_title: chararray,totalapplicaion: long}

------------------------------------------------------------------------------------------------------------------------------------


filterh1b1 = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN';

--DESCRIBE filterh1b1;
--filterh1b1: {case_status: chararray,job_title: chararray}


successgrouped1 = GROUP filterh1b1 BY job_title;

-- DESCRIBE successgrouped1;
--successgrouped1: {group: chararray,filterh1b1: {(case_status: chararray,job_title: chararray)}}


successcount1 = FOREACH successgrouped1 GENERATE group AS job_title,COUNT(filterh1b1) AS totalsuccess1;

--DESCRIBE successcount1;
--successcount1: {job_title: chararray,totalsuccess1: long}

-------------------------------------------------------------------------------------------------------------------------------------


filterh1b2 = FILTER finalh1b BY case_status == 'CERTIFIED';

successgrouped2 = GROUP filterh1b2 BY job_title;

successcount2 = FOREACH successgrouped2 GENERATE group AS job_title,COUNT(filterh1b2) AS totalsuccess2;


-------------------------------------------------------------------------------------------------------------------------------------

joined = JOIN allcount BY $0, successcount1 BY $0, successcount2 BY $0;

--DESCRIBE joined;
--joined: {allcount::job_title: chararray,allcount::totalapplicaion: long,successcount1::job_title: chararray,successcount1::totalsuccess1: long,successcount2::job_title: chararray,successcount2::totalsuccess2: long}


finalbag = FOREACH joined GENERATE $0 AS job_title, $1 as petitions, (FLOAT)((($3+$5)*100)/$1) AS successrate;

--DESCRIBE finalbag;
--finalbag: {job_title: chararray,petitions: long,successrate: float}


filtersuccessrate = FILTER finalbag BY petitions >= 1000 AND successrate > 70;

--filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;

finaloutput = ORDER filtersuccessrate BY successrate DESC;

--finaloutput = ORDER filtersuccessrate BY $2 DESC;

STORE finaloutput INTO '/H1BVisaProject/Pig/Q10_JobPositionsWithNoPetititionsHavingSuccessRateAbove70Percent' USING PigStorage();

DUMP finaloutput;


