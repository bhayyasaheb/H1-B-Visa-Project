
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

finalh1b = FOREACH h1b GENERATE case_status,employer_name;

--DESCRIBE finalh1b;
--finalh1b: {case_status: chararray,employer_name: chararray}

---------------------------------------------------------------------------------------------------------------------------------------

allgrouped = GROUP finalh1b BY employer_name;

--DESCRIBE allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,employer_name: chararray)}}


allcount = FOREACH allgrouped GENERATE group AS employer_name,COUNT(finalh1b) AS totalapplicaion;

--DESCRIBE allcount;
--allcount: {employer_name: chararray,totalapplication: long}

--------------------------------------------------------------------------------------------------------------------------------------

filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

--DESCRIBE filterh1b;
--filterh1b: {case_status: chararray,employer_name: chararray}


successgrouped = GROUP filterh1b BY employer_name;

--DESCRIBE successgrouped;
--successgrouped: {group: chararray,filterh1b: {(case_status: chararray,employer_name: chararray)}}


successcount = FOREACH successgrouped GENERATE group AS employer_name,COUNT(filterh1b) AS totalsuccess;

-- DESCRIBE successcount;  
--successcount: {employer_name: chararray,totalsuccess: long}


---------------------------------------------------------------------------------------------------------------------------------------


joined = JOIN allcount BY $0, successcount BY $0;

--DESCRIBE joined;
--joined: {allcount::employer_name: chararray,allcount::totalapplicaion: long,successcount::employer_name: chararray,successcount::totalsuccess: long}



finalbag = FOREACH joined GENERATE $0 AS employer_name, $1 as petitions, (FLOAT)(($3*100)/$1) AS successrate;

--DESCRIBE finalbag;
--finalbag: {employer_name: chararray,petitions: long,successrate: float}

								
filtersuccessrate = FILTER finalbag BY petitions >= 1000 AND successrate > 70;
			
--filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;


finaloutput = ORDER filtersuccessrate BY successrate DESC;

--finaloutput = ORDER filtersuccessrate BY $2 DESC;


STORE finaloutput INTO '/H1BVisaProject/Pig/Q9_EmployersWithNoPetititionsHavingSuccessRateAbove70Percent' USING PigStorage();


DUMP finaloutput;
--(HTC GLOBAL SERVICES, INC.,1164,100.0)
--(HTC GLOBAL SERVICES INC.,2632,100.0)
--(YASH TECHNOLOGIES, INC.,2214,99.0)
--(YASH & LUJAN CONSULTING, INC.,1372,99.0)
--(TECH MAHINDRA (AMERICAS),INC.,10732,99.0)


