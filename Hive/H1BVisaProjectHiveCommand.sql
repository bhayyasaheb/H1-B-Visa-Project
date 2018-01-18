
Q1 a) Is the number of petitions with Data Engineer job title increasing over time?
Ans:- 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q1A_DataEnggJob.sql

-----------------------------------------------------------------------------------------------------------------------------------------------

Q2 b) find top 5 locations in the US who have got certified visa for each year.[certified]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q2B_Top5LocationsInUSGotCertifiedVisaForEahYear.sql 

---------------------------------------------------------------------------------------------------------------------------------------------

3)Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q3_IndustryHasMostDataScientistPositions.sql

---------------------------------------------------------------------------------------------------------------------------------------------

7) Create a bar graph to depict the number of applications for each year [All]
Ans:-

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q7_NumberOfApplicationsForEachYear.sql

---------------------------------------------------------------------------------------------------------------------------------------------

8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order [Certified and Certified Withdrawn.]
Ans:-

Q8 A. For Full Time 

hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q8A_AvgPrevailingWageForEachJobAndYearFullTime.sql


Q8 B. For Part Time
hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q8B_AvgPrevailingWageForEachJobAndYearPartTime.sql 

----------------------------------------------------------------------------------------------------------------------------------------------
