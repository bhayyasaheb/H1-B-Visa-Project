
--10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?


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


finalh1b = FOREACH h1b GENERATE case_status,job_title;

--DESCRIBE finalh1b;
--finalh1b: {case_status: chararray,job_title: chararray}

--------------------------------------------------------------------------------------------------------------------------------------

allgrouped = GROUP finalh1b BY job_title;

--DESCRIBE allgrouped;
--allgrouped: {group: chararray,finalh1b: {(case_status: chararray,job_title: chararray)}}


allcount = FOREACH allgrouped GENERATE group as job_title,COUNT(finalh1b.case_status) as totalapplicaion;

--DESCRIBE allcount;
--allcount: {job_title: chararray,totalapplicaion: long}

-----------------------------------------------------------------------------------------------------------------------------------


filterh1b = FILTER finalh1b BY case_status == 'CERTIFIED-WITHDRAWN' OR case_status == 'CERTIFIED';

--DESCRIBE filterh1b;
--filterh1b: {case_status: chararray,job_title: chararray}


successgrouped = GROUP filterh1b BY job_title;

-- DESCRIBE successgrouped;
--successgrouped: {group: chararray,filterh1b: {(case_status: chararray,job_title: chararray)}}


successcount = FOREACH successgrouped GENERATE group AS job_title,COUNT(filterh1b.case_status) as totalsuccess;

--DESCRIBE successcount;
--successcount: {job_title: chararray,totalsuccess: long}


-------------------------------------------------------------------------------------------------------------------------------

joined = JOIN allcount BY $0,successcount BY $0;

--DESCRIBE joined;
--joined: {allcount::job_title: chararray,allcount::totalapplicaion: long,successcount::job_title: chararray,successcount::totalsuccess: long}


finalbag = FOREACH joined GENERATE $0,$1 as petitions, (($3*100)/$1) AS successrate;

--DESCRIBE finalbag;
--finalbag: {job_title: chararray,petitions: long,successrate: float}

											
filtersuccessrate = FILTER finalbag BY $1 >= 1000 AND $2 > 70;


finaloutput = ORDER filtersuccessrate BY $2 DESC;



--STORE finaloutput INTO '/home/hduser/h1b/Pig/Q10_JobPositionsNoPetitionsSuccessRateMore70';

DUMP finaloutput;

/*
(COMPUTER PROGRAMMER / CONFIGURER 2,1276,100)
(PRODUCTION SUPPORT LEAD - US,1301,100)
(PROGRAMMER ANALYST - II,3588,99)
(LEAD CONSULTANT - US,3402,99)
(PROJECT MANAGER - US,7046,99)
(PRODUCTION SUPPORT ANALYST - US,1451,99)
(SYSTEMS ANALYST - II,1339,99)
(TECHNOLOGY LEAD - US,28350,99)
(PROJECT MANAGER - III,1651,99)
(PROGRAMMER ANALYST - I,1432,99)
(SYSTEMS ANALYST - III,1006,99)
(SYSTEMS ENGINEER - US,10036,99)
(ASSURANCE STAFF,2334,99)
(CONSULTANT - US,7426,99)
(COMPUTER SPECIALIST/TESTING AND QUALITY ANALYST 2,3998,99)
(SENIOR PROJECT MANAGER - US,2774,99)
(COMPUTER SPECIALIST/SYSTEM SUPPORT AND DEVELOPMENT,1339,99)
(SPECIALIST MASTER,1119,99)
(COMPUTER SYSTEMS ANALYST 3,2170,99)
(COMPUTER SYSTEMS ANALYST 2,4031,99)
(TECHNOLOGY ARCHITECT - US,4707,99)
(TEST ANALYST - US,4958,99)
(PRINCIPAL CONSULTANT - US,1352,99)
(SENIOR TECHNOLOGY ARCHITECT - US,1417,99)
(DATA WAREHOUSE SPECIALIST,1631,99)
(COMPUTER PROGRAMMER/CONFIGURER 3,1145,99)
(ASSOCIATE CONSULTANT - US,4393,99)
(TECHNICAL TEST LEAD - US,5374,99)
(TEST ENGINEER - US,2198,99)
(DEVELOPER USER INTERFACE,5247,99)
(COMPUTER PROGRAMMER/CONFIGURER 2,6729,99)
(TECHNOLOGY ANALYST - US,26055,99)
(COMPUTER SPECIALIST/SYSTEM SUPPORT AND DEVELOPMENT ADMIN 2,1085,99)
(PROGRAMMER/DEVELOPER,1560,98)
(TEST CONSULTANT,1454,98)
(ADVISORY MANAGER,3255,98)
(SOFTWARE QUALITY ASSURANCE ENGINEER AND TESTER,1568,98)
(COMPUTER SYSTEMS ENGINEER/ARCHITECT,2067,98)
(ARCHITECT LEVEL 2,2892,98)
(MODULE LEAD,2226,98)
(SPECIALIST SENIOR,1447,98)
(AUDIT ASSISTANT,1205,98)
(DEVELOPER,12909,98)
(CONSULTANT LEVEL 3,1171,98)
(AUDIT SENIOR,1070,98)
(LEAD ENGINEER,11157,98)
(TEST ENGINEER LEVEL 2,2372,98)
(ADVISORY STAFF,2413,98)
(SOFTWARE ENGINEER AND TESTER,1216,98)
(ADVISORY SENIOR,5416,98)
(COMPUTER SPECIALIST,2175,98)
(ERS SENIOR CONSULTANT,2249,97)
(BUSINESS TECHNOLOGY ANALYST,2005,97)
(FUNCTIONAL CONSULTANT,1115,97)
(SOFTWARE DEVELOPMENT ENGINEER IN TEST,4258,97)
(ERS CONSULTANT,2170,97)
(TAX SENIOR,1838,97)
(TECHNICAL ANALYST,2932,97)
(PROGRAMMER ANALYST LEVEL 1,2395,97)
(SOFTWARE ENGINEER 2,4166,97)
(ADVISORY SENIOR ASSOCIATE,1332,97)
(TEST ENGINEER LEVEL 1,1036,97)
(APPLICATIONS CONSULTANT,1180,96)
(PROGRAMMER/ANALYST,9375,96)
(APPLICATIONS DEVELOPER,3366,96)
(SOLUTIONS ARCHITECT,1915,96)
(BUSINESS SYSTEM ANALYST,4435,96)
(SOFTWARE DEVELOPMENT ENGINEER,7284,96)
(SYSTEM ADMINISTRATOR,5048,96)
(SOFTWARE DESIGN ENGINEER,1080,96)
(QA TESTER,1170,96)
(SENIOR SOFTWARE DEVELOPER,10208,96)
(SENIOR PROGRAMMER ANALYST,5810,96)
(ELEMENTARY BILINGUAL TEACHER,2088,96)
(PROGRAMMER ANALYST,249038,96)
(LEAD CONSULTANT,2169,96)
(RESEARCH FELLOW,5981,96)
(ASSOCIATE RESEARCH SCIENTIST,1400,96)
(ASSISTANT RESEARCH SCIENTIST,1103,96)
(ORACLE DATABASE ADMINISTRATOR,1527,96)
(PROGRAMMER ANALYSTS,1133,96)
(COMPUTER SYSTEMS ENGINEER,11090,96)
(SENIOR ASSOCIATE,3540,96)
(ASSISTANT VICE PRESIDENT,2132,96)
(SYSTEMS ANALYSTS,1252,96)
(PRINCIPAL CONSULTANT,1836,96)
(CONSULTANT,23081,96)
(SR. SYSTEMS ANALYST,1151,95)
(PHYSICIAN IN A POST GRADUATE TRAINING PROGRAM,2421,95)
(SENIOR SOFTWARE DEVELOPMENT ENGINEER,1399,95)
(SOFTWARE QUALITY ASSURANCE ENGINEER,4920,95)
(SOFTWARE DEVELOPMENT ENGINEER II,3274,95)
(POSTDOCTORAL RESEARCH ASSOCIATE,6041,95)
(SOFTWARE APPLICATION ENGINEER,1126,95)
(SENIOR DATABASE ADMINISTRATOR,1229,95)
(SENIOR TECHNICAL CONSULTANT,1882,95)
(PRINCIPAL SOFTWARE ENGINEER,2257,95)
(COMPUTER PROGRAMMER/ANALYST,1122,95)
(COMPUTER PROGRAMMER ANALYST,13634,95)
(ASSOCIATE SOFTWARE ENGINEER,1215,95)
(SOFTWARE ENGINEER & TESTER,1538,95)
(QUALITY ASSURANCE ANALYST,7326,95)
(MEMBER OF TECHNICAL STAFF,1774,95)
(COMPUTER SYSTEMS ANALYSTS,4728,95)
(BUSINESS SYSTEMS ANALYST,10110,95)
(SENIOR BUSINESS ANALYST,3402,95)
(SR. PROGRAMMER ANALYST,3716,95)
(SENIOR SYSTEMS ANALYST,5353,95)
(POSTDOCTORAL ASSOCIATE,5145,95)
(DATABASE ADMINISTRATOR,16665,95)
(SOFTWARE ENGINEER III,1328,95)
(TECHNICAL SPECIALIST,1295,95)
(SOFTWARE QA ENGINEER,1169,95)
(PROGRAMMER / ANALYST,1173,95)
(POSTDOCTORAL SCHOLAR,3186,95)
(TECHNICAL ARCHITECT,2908,95)
(SOFTWARE QA ANALYST,1112,95)
(SOFTWARE PROGRAMMER,3577,95)
(SOFTWARE ENGINEER 3,1891,95)
(COMPUTER PROGRAMMER,70570,95)
(SYSTEMS ANALYST II,1036,95)
(SOFTWARE ENGINEER,121307,95)
(SENIOR CONSULTANT,24904,95)
(SYSTEMS ENGINEER,8078,95)
(SOFTWARE ANALYST,1072,95)
(DATABASE ANALYST,1050,95)
(BUSINESS ANALYST,39681,95)
(ASSURANCE SENIOR,1607,95)
(SYSTEMS ANALYST,61965,95)
(SCIENCE TEACHER,1127,95)
(QUALITY ANALYST,2616,95)
(PROGRAM MANAGER,3920,95)
(SENIOR MANAGER,1439,95)
(SAP CONSULTANT,3023,95)
(LEAD DEVELOPER,1049,95)
(JAVA DEVELOPER,7596,95)
(DATA SCIENTIST,1932,95)
(.NET DEVELOPER,2921,95)
(WEB DEVELOPER,8024,95)
(ETL DEVELOPER,1841,95)
(TEST ANALYST,1419,95)
(PROJECT LEAD,2363,95)
(QA ANALYST,6871,95)
(ASSOCIATE,12502,95)
(ARCHITECT,4982,95)
(MANAGER,8561,95)
(ANALYST,11751,95)
(STAFF SOFTWARE ENGINEER,2976,94)
(PROJECT MANAGER,20172,94)
(COMPUTER SOFTWARE ENGINEER, APPLICATIONS,4426,94)
(STAFF SCIENTIST,1242,94)
(HARDWARE ENGINEER,2556,94)
(ASSOCIATE CONSULTANT,1350,94)
(NETWORK ENGINEER,5422,94)
(TECHNICAL SUPPORT ENGINEER,1230,94)
(PROGRAMMER,6011,94)
(QA ENGINEER,2224,94)
(APPLICATION ENGINEER,1458,94)
(SOFTWARE ENGINEER II,2051,94)
(COMPUTER SYSTEMS ANALYST,35086,94)
(PROGRAMMER/ ANALYST,1000,94)
(SR. SOFTWARE DEVELOPER,1161,94)
(POSTDOCTORAL FELLOW,7857,94)
(SOFTWARE TEST ENGINEER,3591,94)
(TECHNICAL CONSULTANT,3420,94)
(SENIOR PRODUCT MANAGER,1085,94)
(PROGRAMMER ANALYST II,1059,94)
(SENIOR JAVA DEVELOPER,1395,94)
(SR. SOFTWARE ENGINEER,4863,94)
(SENIOR HARDWARE ENGINEER,1653,94)
(ENGINEER II,1249,94)
(VALIDATION ENGINEER,1159,94)
(ASSISTANT PROFESSOR,25265,94)
(SENIOR SOFTWARE ENGINEER,27133,94)
(SOFTWARE DEVELOPER, APPLICATIONS,1830,94)
(SYSTEMS ADMINISTRATOR,6659,94)
(SOFTWARE DEVELOPER,42907,94)
(SOFTWARE ARCHITECT,1878,94)
(TEST ENGINEER,3936,94)
(TECHNICAL RECRUITER,1364,94)
(RESEARCH ASSOCIATE,13623,94)
(RF ENGINEER,2794,94)
(POSTDOCTORAL RESEARCHER,2130,94)
(LECTURER,2257,94)
(PRINCIPAL ENGINEER,1066,94)
(BUSINESS INTELLIGENCE ANALYST,1972,94)
(VISITING ASSISTANT PROFESSOR,1311,94)
(STAFF ENGINEER,1869,94)
(IT PROJECT MANAGER,2473,94)
(DATABASE DEVELOPER,1155,94)
(DATA ANALYST,3805,94)
(SYSTEM ANALYST,4684,94)
(VICE PRESIDENT,3159,94)
(SENIOR SYSTEMS ENGINEER,2030,94)
(RESEARCH ASSISTANT PROFESSOR,1973,94)
(CLINICAL ASSISTANT PROFESSOR,1281,94)
(QUALITY ASSURANCE ENGINEER,3647,94)
(RESEARCHER,1031,94)
(PRODUCT MANAGER,3367,94)
(COMPUTER PROGRAMMERS,4963,93)
(SOFTWARE DEVELOPMENT ENGINEER I,2128,93)
(ASSOCIATE PROFESSOR,1441,93)
(RESEARCH SCIENTIST,5154,93)
(RESEARCH ASSISTANT,1777,93)
(COMPONENT DESIGN ENGINEER,2851,93)
(SENIOR RESEARCH ASSOCIATE,1015,93)
(TECHNICAL MANAGER,1060,93)
(RESEARCH ENGINEER,1338,93)
(PHARMACIST,5864,93)
(SENIOR DEVELOPER,2994,93)
(QUALITY ENGINEER,2381,93)
(PRODUCT ENGINEER,2634,93)
(INSTRUCTOR,3014,93)
(SENIOR SYSTEMS ANALYST JC60,3069,93)
(SPEECH LANGUAGE PATHOLOGIST,1381,93)
(SENIOR ENGINEER,3773,93)
(DESIGN ENGINEER,6325,93)
(POSTDOCTORAL RESEARCH FELLOW,2346,93)
(TECHNICAL LEAD,5218,93)
(SENIOR ANALYST,1646,93)
(NETWORK AND COMPUTER SYSTEMS ADMINISTRATOR,1928,93)
(ENGINEER,4941,93)
(IT CONSULTANT,3497,93)
(LEAD SOFTWARE ENGINEER,1572,93)
(MANUFACTURING ENGINEER,1906,93)
(SENIOR DESIGN ENGINEER,1209,93)
(NETWORK ADMINISTRATOR,2624,93)
(SYSTEMS ANALYST JC65,3321,93)
(QUANTITATIVE ANALYST,1293,93)
(POST DOCTORAL FELLOW,1507,93)
(SOLUTION ARCHITECT,1994,92)
(SYSTEM ENGINEER,2145,92)
(PROJECT ENGINEER,6439,92)
(HOSPITALIST PHYSICIAN,4067,92)
(SENIOR PROJECT MANAGER,1015,92)
(COMPUTER SYSTEM ANALYST,3753,92)
(TEST LEAD,1726,92)
(DIRECTOR,1333,92)
(APPLICATIONS ENGINEER,1688,92)
(OCCUPATIONAL THERAPIST,4437,92)
(SENIOR FINANCIAL ANALYST,1196,92)
(CLINICAL FELLOW,1146,92)
(HOSPITALIST,4387,92)
(SOFTWARE DEVELOPERS, APPLICATIONS,1195,92)
(SENIOR APPLICATION DEVELOPER,1965,92)
(PHYSICAL THERAPIST,20207,92)
(INDUSTRIAL DESIGNER,3619,92)
(SCIENTIST,1340,91)
(PSYCHIATRIST,1289,91)
(PHYSICIAN,4417,91)
(MEDICAL RESIDENT,2336,91)
(MANAGER JC50,1874,91)
(COMPUTER SOFTWARE ENGINEER,2684,91)
(TECHNICAL PROJECT MANAGER,1052,91)
(PROCESS ENGINEER,4377,91)
(APPLICATION DEVELOPER,7692,91)
(RESIDENT,1245,91)
(TEST SPECIALIST,1011,90)
(SENIOR SYSTEM ENGINEER,1408,90)
(SPECIAL EDUCATION TEACHER,1721,90)
(ACCOUNT MANAGER,1066,90)
(ENGINEERING MANAGER,1199,90)
(STRUCTURAL ENGINEER,1094,90)
(MECHANICAL ENGINEER,7301,90)
(MANAGING CONSULTANT,3873,90)
(APPLICATION PROGRAMMER,1686,90)
(RESEARCH ANALYST,1869,90)
(DESIGNER,1992,89)
(ASSOCIATE ATTORNEY,1533,89)
(SALES ENGINEER,2167,89)
(RESIDENT PHYSICIAN,2119,89)
(IT SPECIALIST,2585,89)
(ARCHITECTURAL DESIGNER,2334,89)
(PEDIATRICIAN,1214,89)
(MEDICAL TECHNOLOGIST,3125,89)
(INDUSTRIAL ENGINEER,2093,89)
(OPERATIONS RESEARCH ANALYST,1946,89)
(NEPHROLOGIST,1027,89)
(ELECTRICAL ENGINEER,4174,88)
(ELECTRONICS ENGINEER,1060,88)
(CHEMIST,1380,88)
(MARKETING ANALYST,1573,88)
(BUSINESS DEVELOPMENT MANAGER,2345,88)
(MANAGEMENT ANALYST,5386,88)
(ELEMENTARY SCHOOL TEACHER,1304,87)
(BUDGET ANALYST,1687,87)
(BUSINESS DEVELOPMENT ANALYST,1148,87)
(STAFF ACCOUNTANT,4491,87)
(BUSINESS DEVELOPMENT SPECIALIST,1482,87)
(FINANCIAL ANALYST,8515,87)
(SYSTEM ANALYST JC65,1419,86)
(MARKETING SPECIALIST,2150,86)
(TEACHER,3576,86)
(DENTIST,3250,86)
(BUSINESS OPERATIONS SPECIALIST,1034,85)
(ATTORNEY,1050,85)
(INTERIOR DESIGNER,1361,85)
(CIVIL ENGINEER,2257,84)
(MARKET RESEARCH ANALYST,8934,84)
(LAW CLERK,1709,83)
(OPERATIONS MANAGER,1785,83)
(GRAPHIC DESIGNER,5020,83)
(ACCOUNTANT,14048,83)
(SALES MANAGER,1232,83)
(PUBLIC RELATIONS SPECIALIST,1931,82)
(FINANCIAL MANAGER,1080,82)
(CHIEF EXECUTIVE OFFICER,1095,80)
(MARKETING MANAGER,2230,80)
(GENERAL MANAGER,1348,78)
*/

