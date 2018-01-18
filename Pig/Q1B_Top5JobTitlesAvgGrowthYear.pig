
--Q 1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]

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


jobtitle = FOREACH h1b GENERATE year,job_title,case_status,worksite;

--DESCRIBE jobtitle;
--jobtitle: {year: chararray,job_title: chararray,case_status: chararray,worksite: chararray}

--------------------------------------------------------------------------------------------------------------------------------------------

year1 = FILTER jobtitle BY year == '2011';

--DESCRIBE year1;                         
--year1: {year: chararray,job_title: chararray,case_status: chararray,worksite: chararray}


group1 = GROUP year1 BY job_title;

--DESCRIBE group1;           
--group1: {group: chararray,year1: {(year: chararray,job_title: chararray,case_status: chararray,worksite: chararray)}}


count1 = FOREACH group1 GENERATE group AS job_title,COUNT($1) AS count2011;

--DESCRIBE count1;;                                                          
--count1: {job_title: chararray,count2011: long}

--DUMP count1;
--(WORLDWIDE FIELD OPERATIONS PROF. SERVICES ENGINEER,2)
--(WORLDWIDE PROJECT DIRECTOR, BIOLOGICS DELIVERY SYS,1)
--(WORLDWIDE SOFTWARE GROUP SERVICES ACQUISITIONS PRO,1)
--(WS SERVICE SR SPECIALIST; FINANCIAL TOOLS DEVELOPE,1)
--(XI DEVELOPER - COMPUTER SOFTWARE ENGINEER APPLICAT,1)

-----------------------------------------------------------------------------------------------------------------------------------------------

year2 = FILTER jobtitle BY $0 == '2012';

group2 = GROUP year2 BY $1;

count2 = FOREACH group2 GENERATE group AS job_title,COUNT($1) AS count2012;


year3 = FILTER jobtitle BY $0 == '2013';

group3 = GROUP year3 BY $1;

count3 = FOREACH group3 GENERATE group AS job_title,COUNT($1) AS count2013;



year4 = FILTER jobtitle BY $0 == '2014';

group4 = GROUP year4 BY $1;

count4 = FOREACH group4 GENERATE group AS job_title,COUNT($1) AS count2014;



year5 = FILTER jobtitle BY $0 == '2015';

group5 = GROUP year5 BY $1;

count5 = FOREACH group5 GENERATE group AS job_title,COUNT($1) AS count2015;



year6 = FILTER jobtitle BY $0 == '2016';

group6 = GROUP year6 BY $1;

count6 = FOREACH group6 GENERATE group AS job_title,COUNT($1) AS count2016;

-------------------------------------------------------------------------------------------------------------------------------------------


joined = JOIN count1 BY $0, count2 BY $0, count3 BY $0, count4 BY $0, count5 BY $0, count6 BY $0;

--DESCRIBE joined;
--joined: {count1::job_title: chararray,count1::count2011: long,count2::job_title: chararray,count2::count2012: long,count3::job_title: chararray,count3::count2013: long,count4::job_title: chararray,count4::count2014: long,count5::job_title: chararray,count5::count2015: long,count6::job_title: chararray,count6::count2016: long}

application = FOREACH joined GENERATE $0 AS job_title,$1 AS count2011,$3 AS count2012,$5 AS count2013,$7 AS count2014,$9 AS count2015,$11 AS count2016;

--DESCRIBE application;
--application: {job_title: chararray,count2011: long,count2012: long,count2013: long,count2014: long,count2015: long,count2016: long}

growth = FOREACH application GENERATE job_title, 
(FLOAT)($2-$1)*100/$1 AS growth2012, 
(FLOAT)($3-$2)*100/$2 AS growth2013, 
(FLOAT)($4-$3)*100/$3 AS growth2014, 
(FLOAT)($5-$4)*100/$4 AS growth2015, 
(FLOAT)($6-$5)*100/$5 AS growth2016;

--DESCRIBE growth;
--growth: {job_title: chararray,growth2012: float,growth2013: float,growth2014: float,growth2015: float,growth2016: float}


avggrowth= foreach growth generate $0,($1+$2+$3+$4+$5)/5 AS averagegrowth;

--DESCRIBE avggrowth;
--avggrowth: {job_title: chararray,averagegrowth: float}

top5jobtitle = LIMIT (ORDER avggrowth BY $1 DESC) 5;

--DESCRIBE top5jobtitle;
--top5jobtitle: {job_title: chararray,averagegrowth: float}



DUMP top5jobtitle;
--(SENIOR SYSTEMS ANALYST JC60,4255.4644)
--(SOFTWARE DEVELOPER 2,3480.5925)
--(PROJECT MANAGER 3,3233.3335)
--(SYSTEMS ANALYST JC65,2984.8809)
--(MODULE LEAD,2917.112)




