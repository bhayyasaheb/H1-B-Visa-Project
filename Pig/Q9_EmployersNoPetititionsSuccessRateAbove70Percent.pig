
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

filterh1b1 = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN';

--DESCRIBE filterh1b1;
--filterh1b1: {case_status: chararray,employer_name: chararray}


successgrouped1 = GROUP filterh1b1 BY employer_name;

--DESCRIBE successgrouped1;
--successgrouped1: {group: chararray,filterh1b1: {(case_status: chararray,employer_name: chararray)}}


successcount1 = FOREACH successgrouped1 GENERATE group AS employer_name,COUNT(filterh1b1) AS totalsuccess1;

-- DESCRIBE successcount1;  
--successcount1: {employer_name: chararray,totalsuccess1: long}


---------------------------------------------------------------------------------------------------------------------------------------

filterh1b2 = FILTER finalh1b BY case_status == 'CERTIFIED';

successgrouped2 = GROUP filterh1b2 BY employer_name;

successcount2 = FOREACH successgrouped2 GENERATE group AS employer_name,COUNT(filterh1b2) AS totalsuccess2;

-------------------------------------------------------------------------------------------------------------------------------------


joined = JOIN allcount BY $0, successcount1 BY $0, successcount2 BY $0;

--DESCRIBE joined;
--joined: {allcount::employer_name: chararray,allcount::totalapplicaion: long,successcount1::employer_name: chararray,successcount1::totalsuccess1: long,successcount2::employer_name: chararray,successcount2::totalsuccess2: long}



finalbag = FOREACH joined GENERATE $0,$1 as petitions, (FLOAT)((($3+$5)*100)/$1) AS successrate;

--DESCRIBE finalbag;
--finalbag: {allcount::employer_name: chararray,petitions: long,successrate: float}

								
filtersuccessrate = FILTER finalbag BY petitions >= 1000 AND successrate > 70;
			
--filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;


finaloutput = ORDER filtersuccessrate BY successrate DESC;

--finaloutput = ORDER filtersuccessrate BY $2 DESC;


STORE finaloutput INTO '/H1BVisaProject/Pig/Q9_ EmployersWithNoPetititionsHavingSuccessRateAbove70Percent' USING PigStorage();


DUMP finaloutput;
--Top 10 employee name whose suceess rate > 70% and petitions >=1000
--(HCL AMERICA, INC.,22678,99.0)
--(RELIABLE SOFTWARE RESOURCES, INC.,1992,99.0)
--(TATA CONSULTANCY SERVICES LIMITED,64726,99.0)
--(ERP ANALYSTS, INC.,1785,99.0)
--(PATNI AMERICAS INC.,3149,99.0)
--(ACCENTURE LLP,33447,99.0)
--(KFORCE INC.,1596,99.0)
--(NTT DATA, INC.,4611,99.0)
--(INFOSYS LIMITED,130592,99.0)
--(BLOOMBERG, LP,2352,98.0)


