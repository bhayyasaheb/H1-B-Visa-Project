
--9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ? Using MapReduce mode of Pig


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
/*
(HTC GLOBAL SERVICES, INC.,1164,100)
(HTC GLOBAL SERVICES INC.,2632,100)
(YASH TECHNOLOGIES, INC.,2214,99)
(YASH & LUJAN CONSULTING, INC.,1372,99)
(TECH MAHINDRA (AMERICAS),INC.,10732,99)
(ACCENTURE LLP,33447,99)
(TATA CONSULTANCY SERVICES LIMITED,64726,99)
(MINDTREE LIMITED,4067,99)
(RELIABLE SOFTWARE RESOURCES, INC.,1992,99)
(HCL AMERICA, INC.,22678,99)
(ERP ANALYSTS, INC.,1785,99)
(INFOSYS LIMITED,130592,99)
(PATNI AMERICAS INC.,3149,99)
(KFORCE INC.,1596,99)
(TECH MAHINDRA ( AMERICAS), INC,1170,99)
(DIASPARK, INC.,1419,99)
(NTT DATA, INC.,4611,99)
(PERFICIENT, INC.,1366,98)
(3I INFOTECH, INC.,2041,98)
(ERNST & YOUNG U.S. LLP,18232,98)
(SAP AMERICA, INC.,1456,98)
(PRICEWATERHOUSECOOPERS, LLP,2529,98)
(INOVANT, LLC,1086,98)
(GENESIS ELDERCARE REHABILITATION SERVICES, INC.,1320,98)
(HARVARD UNIVERSITY,1966,98)
(PYRAMID TECHNOLOGY SOLUTIONS, INC,1056,98)
(NVIDIA CORPORATION,1182,98)
(GRANDISON MANAGEMENT, INC.,1386,98)
(CYBERTHINK INC,1618,98)
(HORIZON TECHNOLOGIES INC,1731,98)
(MCKINSEY & COMPANY, INC. UNITED STATES,1097,98)
(AMDOCS INC.,1023,98)
(CGI TECHNOLOGIES AND SOLUTIONS INC.,1995,98)
(AVCO CONSULTING INC,1424,98)
(MPHASIS CORPORATION,5199,98)
(GENPACT LLC,1046,98)
(MASTECH, INC., A MASTECH HOLDINGS, INC. COMPANY,5228,98)
(MICHIGAN STATE UNIVERSITY,1191,98)
(CVS RX SERVICES, INC.,2735,98)
(DALLAS INDEPENDENT SCHOOL DISTRICT,1729,98)
(BLOOMBERG, LP,2352,98)
(COMPUNNEL SOFTWARE GROUP, INC.,3378,98)
(MICROSOFT CORPORATION,25576,98)
(DELOITTE CONSULTING LLP,36742,98)
(CREDIT SUISSE SECURITIES (USA) LLC,2546,98)
(SMARTPLAY, INC.,1377,98)
(BARCLAYS SERVICES CORP.,1605,98)
(THE MATHWORKS, INC.,2020,98)
(DELOITTE TAX LLP,2501,98)
(ALINDUS, INC.,1046,98)
(VEDICSOFT,1169,98)
(THE BOSTON CONSULTING GROUP, INC.,1352,98)
(UNIVERSITY OF PITTSBURGH,1632,98)
(SYNTEL INC,1946,98)
(WIPRO LIMITED,48117,98)
(SYNTEL CONSULTING INC.,3167,98)
(PRICEWATERHOUSECOOPERS ADVISORY SERVICES LLC,1724,97)
(LINKEDIN CORPORATION,2194,97)
(CSC COVANSYS CORPORATION,2251,97)
(UNIVERSITY OF MINNESOTA,1353,97)
(UBER TECHNOLOGIES, INC.,1006,97)
(ORACLE AMERICA, INC.,7684,97)
(RJT COMPUQUEST, INC.,1662,97)
(CAPGEMINI AMERICA INC,16725,97)
(CYIENT, INC.,1281,97)
(HEADSTRONG SERVICES LLC,2587,97)
(ASTIR IT SOLUTIONS INC.,1955,97)
(SCHLUMBERGER TECHNOLOGY CORPORATION,2310,97)
(DELOITTE & TOUCHE LLP,9642,97)
(PHOTON INFOTECH, INC.,1235,97)
(WASHINGTON UNIVERSITY IN ST. LOUIS,1576,97)
(MARLABS, INC,2626,97)
(VMWARE, INC.,2617,97)
(AKVARR INC,1372,97)
(BIRLASOFT INC,2370,97)
(COMCAST CABLE COMMUNICATIONS, LLC,1214,97)
(SATYAM COMPUTER SERVICES LIMITED,2403,97)
(APPLE INC.,7317,97)
(ERICSSON INC.,3359,97)
(MERRILL LYNCH,1873,97)
(CITIBANK, N.A.,2173,97)
(CHILDREN'S HOSPITAL CORPORATION,1017,97)
(TEXAS INSTRUMENTS INCORPORATED,1780,97)
(RITE AID CORP.,1577,97)
(TECH MAHINDRA (AMERICAS) INC.,2102,97)
(UNIVERSITY OF CALIFORNIA, SAN FRANCISCO,1348,97)
(MICROEXCEL, INC,1159,97)
(COMPUTER SCIENCES CORPORATION,1089,97)
(CITIGROUP GLOBAL MARKETS INC.,1435,97)
(LARSEN & TOUBRO TECHNOLOGY SERVICES LTD,1385,97)
(DOTCOM TEAM, LLC,1125,97)
(SATYAM COMPUTER SERVICES LTD,1622,97)
(INFOSYS TECHNOLOGIES LIMITED,1336,97)
(APEX TECHNOLOGY SYSTEMS, INC,1060,97)
(MEMORIAL SLOAN-KETTERING CANCER CENTER,1080,97)
(TESLA MOTORS, INC.,1441,97)
(UNIVERSITY OF UTAH,1069,97)
(THE UNIV. OF ALA. AT BIRMINGHAM (UAB),1288,97)
(KPMG LLP,4629,97)
(NATSOFT CORPORATION,1137,97)
(CAPITAL ONE SERVICES, LLC,2796,97)
(SRS CONSULTING INC.,1150,97)
(SUNERA TECHNOLOGIES, INC,1440,97)
(BANK OF AMERICA N.A.,4282,97)
(INTONE NETWORKS INC.,1575,97)
(AMERICAN EXPRESS TRAVEL RELATED SERVICES COMPANY, INC.,1045,96)
(TRUSTEES OF THE UNIVERSITY OF PENNSYLVANIA,2258,96)
(NEW YORK UNIVERSITY SCHOOL OF MEDICINE,1126,96)
(MASSACHUSETTS INSTITUTE OF TECHNOLOGY,1347,96)
(CAPGEMINI FINANCIAL SERVICES USA INC,4426,96)
(BLACKROCK FINANCIAL MANAGEMENT, INC.,1048,96)
(VERINON TECHNOLOGY SOLUTIONS LTD.,1245,96)
(LARSEN & TOUBRO INFOTECH LIMITED,17457,96)
(UNIVERSITY OF WISCONSIN-MADISON,1115,96)
(SMARTSOFT INTERNATIONAL, INC.,1212,96)
(V-SOFT CONSULTING GROUP, INC,4283,96)
(GENERAL HOSPITAL CORPORATION,1429,96)
(HP ENTERPRISE SERVICES, LLC,1149,96)
(PRICEWATERHOUSECOOPERS LLP,2719,96)
(WAL-MART ASSOCIATES, INC.,3670,96)
(VERIZON DATA SERVICES LLC,1635,96)
(THE UNIVERSITY OF CHICAGO,1277,96)
(RANDSTAD TECHNOLOGIES, LP,3419,96)
(NIIT TECHNOLOGIES LIMITED,1339,96)
(COLLABORATE SOLUTIONS INC,1209,96)
(JOHNS HOPKINS UNIVERSITY,1823,96)
(VEDICSOFT SOLUTIONS LLC,1274,96)
(IGATE TECHNOLOGIES INC.,12564,96)
(HEWLETT-PACKARD COMPANY,1639,96)
(UNIVERSITY OF MICHIGAN,2893,96)
(UNIVERSITY OF ILLINOIS,1196,96)
(HCL GLOBAL SYSTEMS INC,3677,96)
(SYMANTEC CORPORATION,2290,96)
(SALESFORCE.COM, INC.,2245,96)
(JPMORGAN CHASE & CO.,7035,96)
(CHARTER GLOBAL, INC.,1188,96)
(LEAD IT CORPORATION,1720,96)
(COLUMBIA UNIVERSITY,1841,96)
(T-MOBILE USA, INC.,1457,96)
(CERNER CORPORATION,2268,96)
(TECHDEMOCRACY LLC,1027,96)
(PURDUE UNIVERSITY,1076,96)
(EXPERIS US, INC.,1641,96)
(EMC CORPORATION,4467,96)
(SAGARSOFT, INC,1082,96)
(PROKARMA, INC.,1333,96)
(IDHASOFT, INC.,1423,96)
(FACEBOOK, INC.,4145,96)
(TWITTER, INC.,1328,96)
(IDEXCEL, INC.,1360,96)
(PAYPAL, INC.,2830,96)
(GOOGLE INC.,16473,96)
(EBAY INC.,3464,96)
(SATYAM COMPUTER SERVICES LTD.,1694,95)
(THE OHIO STATE UNIVERSITY,1587,95)
(INTRAEDGE, INC.,1254,95)
(THE UNIVERSITY OF IOWA,1569,95)
(EXILANT TECHNOLOGIES PRIVATE LIMITED,1572,95)
(SEARS HOLDINGS MANAGEMENT CORPORATION,1105,95)
(TECHNOSOFT CORPORATION,1625,95)
(NATIONAL INSTITUTES OF HEALTH, HHS,2327,95)
(POLARIS SOFTWARE LAB (INDIA) LTD.,1326,95)
(A2Z DEVELOPMENT CENTER, INC.,1025,95)
(AMAZON CORPORATE LLC,9026,95)
(ADVENT GLOBAL SOLUTIONS INC.,1048,95)
(MULTIVISION INC.,1502,95)
(YAHOO! INC.,3348,95)
(EXPEDIA, INC.,1311,95)
(DELL MARKETING L.P.,1532,95)
(HEXAWARE TECHNOLOGIES, INC.,5466,95)
(UNIVERSITY OF CALIFORNIA, LOS ANGELES,1172,95)
(CYMA SYSTEMS INC,1269,95)
(CMC AMERICAS, INC.,1157,95)
(UST GLOBAL INC.,6363,95)
(SYNECHRON, INC.,3802,95)
(DEUTSCHE BANK SECURITIES INC.,1170,95)
(CIBER, INC.,2097,94)
(BROOKHAVEN NATIONAL LABORATORY,1023,94)
(HITACHI CONSULTING CORPORATION,2854,94)
(ITECH US, INC.,2476,94)
(ORACLE FINANCIAL SERVICES SOFTWARE, INC.,1532,94)
(SAPIENT CORPORATION,2237,94)
(FUJITSU AMERICA, INC.,5309,94)
(QUALCOMM INCORPORATED,3965,94)
(UNIVERSITY OF FLORIDA,1429,94)
(CISCO SYSTEMS, INC.,3140,94)
(KPIT INFOSYSTEMS, INC.,3114,94)
(AT&T SERVICES, INC.,1201,94)
(ADOBE SYSTEMS INCORPORATED,1167,94)
(LARSEN & TOUBRO LIMITED,3066,94)
(MOUNT SINAI MEDICAL CENTER,1114,94)
(UNIVERSITY OF ILLINOIS AT CHICAGO,1131,94)
(MAYO CLINIC,1772,94)
(UNIVERSITY OF WASHINGTON,1187,94)
(QUALCOMM TECHNOLOGIES, INC.,6113,94)
(THE UNIVERSITY OF TEXAS AT AUSTIN,1274,94)
(UNIVERSITY OF CALIFORNIA, DAVIS,1334,94)
(BRIGHAM AND WOMEN'S HOSPITAL,1117,94)
(UNIVERSITY OF COLORADO,1599,94)
(SYSTEM SOFT TECHNOLOGIES LLC,3102,94)
(MANAGEMENT HEALTH SYSTEMS, INC.,2000,94)
(YALE UNIVERSITY,1852,94)
(POPULUS GROUP,2635,94)
(AVANT HEALTHCARE PROFESSIONALS,1006,94)
(HOWARD HUGHES MEDICAL INSTITUTE,1135,94)
(UST GLOBAL INC,6355,94)
(UNIVERSITY OF CALIFORNIA, SAN DIEGO,1202,93)
(GLOBALFOUNDRIES U.S. INC.,1391,93)
(ECLINICALWORKS, LLC,1547,93)
(BAYLOR COLLEGE OF MEDICINE,1666,93)
(CAPGEMINI U.S. LLC,3712,93)
(EMORY UNIVERSITY,1680,93)
(ORION SYSTEMS INTEGRATORS, INC,1160,93)
(TECH MAHINDRA (AMERICAS), INC.,7019,93)
(SOFTWARE PARADIGMS INTERNATIONAL GROUP, LLC,1034,93)
(DUKE UNIVERSITY AND MEDICAL CENTER,1330,93)
(QUALCOMM ATHEROS, INC.,1274,93)
(MICRON TECHNOLOGY, INC.,1934,93)
(NORTHWESTERN UNIVERSITY,1439,93)
(GOLDMAN, SACHS & CO.,3713,93)
(AKAMAI TECHNOLOGIES, INC.,1092,92)
(UNIVERSITY OF MARYLAND COLLEGE PARK,1354,92)
(MORGAN STANLEY & CO. LLC,1669,92)
(INTEL CORPORATION,11415,92)
(COGNIZANT TECHNOLOGY SOLUTIONS U.S. CORPORATION,17528,92)
(INTUIT INC.,1404,92)
(BATTELLE,1052,92)
(PEOPLE TECH GROUP INC.,1124,92)
(ITC INFOTECH (USA), INC.,1859,91)
(ADVANCED MICRO DEVICES, INC.,1512,91)
(THE PENNSYLVANIA STATE UNIVERSITY,1042,91)
(SIRI INFOSOLUTIONS INC.,1039,91)
(MEDTRONIC, INC.,1050,91)
(BALTIMORE CITY PUBLIC SCHOOLS,1014,91)
(L&T TECHNOLOGY SERVICES LIMITED,3722,90)
(HSBC BANK USA, N.A.,1110,90)
(MARVELL SEMICONDUCTOR, INC.,1631,89)
(CUMMINS INC.,4737,89)
(IBM INDIA PVT. LTD.,1284,89)
(INDIANA UNIV. PURDUE UNIV. INDIANAPOLIS,1007,88)
(THE UNIVERSITY OF ARIZONA,1037,88)
(VIRTUSA CORPORATION,2217,88)
(BROADCOM CORPORATION,2862,88)
(IBM CORPORATION,13276,88)
(PERSISTENT SYSTEMS, INC.,3225,87)
(IBM INDIA PRIVATE LIMITED,34219,87)
(AMERICAN INFORMATION TECHNOLOGY CORPORATION,1358,86)
(CITRIX SYSTEMS, INC.,1044,85)
(NETAPP, INC.,1870,84)
*/
