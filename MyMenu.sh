
#!/bin/bash 
show_menu()
{
    NORMAL=`echo "\033[m"`
    MENU=`echo "\033[36m"` #Blue
    NUMBER=`echo "\033[33m"` #yellow
    FGRED=`echo "\033[41m"`
    RED_TEXT=`echo "\033[31m"`
    ENTER_LINE=`echo "\033[33m"`
    echo -e "${MENU}**********************H1B VISA APPLICATIONS**********************${NORMAL}"
    echo -e "${MENU}**${NUMBER} 1)${MENU} Is the number of petitions with Data Engineer job title increasing over time? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 2)${MENU} Find top 5 job titles who are having highest avg growth in applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 3)${MENU} Which part of the US has the most Data Engineer jobs for each year?  ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 4)${MENU} find top 5 locations in the US who have got certified visa for each year.[certified] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 5)${MENU} Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 6)${MENU} Which top 5 employers file the most petitions each year? - Case Status - ALL ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 7)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year? For All Applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 8)${MENU} Find the most popular top 10 job positions for H1B visa applications for each year? Only for Certified Applications. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 9)${MENU} Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time. ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 10)${MENU} Create a bar graph to depict the number of applications for each year [All] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 11)${MENU} Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.] ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 12)${MENU} Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 13)${MENU} Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)? ${NORMAL}"
    echo -e "${MENU}**${NUMBER} 14)${MENU} Export result for question no 10 to MySql database. ${NORMAL}"
    echo -e "${MENU}************************************************************${NORMAL}"
    echo -e "${ENTER_LINE}Please enter a menu option and enter or ${RED_TEXT} enter to exit. ${NORMAL}"
    read opt
}

function option_picked() 
{
    COLOR='\033[01;31m' # bold red
    RESET='\033[00;00m' # normal white
    MESSAGE="$1"  #modified to post the correct option selected
    echo -e "${COLOR}${MESSAGE}${RESET}"
}

clear
show_menu
while [ opt != '' ]
    do
    if [[ $opt = "" ]]; then 
            exit;
    else
        case $opt in
        1) clear;
        option_picked "1 a) Is the number of petitions with Data Engineer job title increasing over time?";
        hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q1A_DataEnggJob.sql
        show_menu;

        ;;


        2) clear;
        option_picked "1 b) Find top 5 job titles who are having highest avg growth in applications.[ALL]";
        pig -x local /home/hduser/Desktop/H1BVisaProject/Pig/Q1B_Top5JobTitlesAvgGrowthYear.pig
        show_menu;
   
        ;;

        
       3) clear;
       option_picked "2 a) Which part of the US has the most Data Engineer jobs for each year?";
       echo -e "Enter the year (ALL | 2011,2012,2013,2014,2015,2016)"
       read var
       echo  "US Has Most Data Engineer Job For ${var}"
       hadoop fs -rm -r /H1BVisaProject/Q2A_WorksiteInUSForDataEngineerJob
       hadoop jar /home/hduser/h1bvisa.jar com.niit.h1bvisa.Q2A_WorksiteInUSForDataEngineerJob /user/hive/warehouse/h1b.db/h1b_final /H1BVisaProject/Q2A_WorksiteInUSForDataEngineerJob $var;
       hadoop fs -cat /H1BVisaProject/Q2A_WorksiteInUSForDataEngineerJob/p*
       show_menu;

       ;;


       4) clear;
       option_picked "2 b) find top 5 locations in the US who have got certified visa for each year.[certified]";
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read var
       echo  "Top 5 locations in the US who have got certified visa for ${var}"
       hive -e "SELECT year, worksite, COUNT(case_status) AS certifiedcount FROM h1b.h1b_final WHERE year = $var AND case_status = 'CERTIFIED' GROUP BY year, worksite ORDER BY certifiedcount DESC LIMIT 5;";
       show_menu;

       ;;
       

       5) clear; 
       option_picked "3) Which industry(SOC_NAME) has the most number of Data Scientist positions? [certified]";
       hive -f /home/hduser/Desktop/H1BVisaProject/Hive/Q3_IndustryHasMostDataScientistPositions.sql
       show_menu;
       
       ;;


       6) clear;
       option_picked "4) Which top 5 employers file the most petitions each year? - Case Status - ALL";
       echo -e "Enter the year (ALL | 2011,2012,2013,2014,2015,2016)"
       read var
       echo  "Top 5 Employers file the most petitions for  ${var}"
       hadoop fs -rm -r /H1BVisaProject/Q4_Top5EmployersWithMostPetitions
       hadoop jar /home/hduser/h1bvisa.jar com.niit.h1bvisa.Q4_Top5EmployersWithMostPetitions /user/hive/warehouse/h1b.db/h1b_final /H1BVisaProject/Q4_Top5EmployersWithMostPetitions $var;
       hadoop fs -cat /H1BVisaProject/Q4_Top5EmployersWithMostPetitions/p*
       show_menu;

       ;;


       7) clear;
       option_picked "5 a) Find the most popular top 10 job positions for H1B visa applications for each year? For all the applications.";
       echo -e "Enter the year (ALL | 2011,2012,2013,2014,2015,2016)"
       read var
       echo  "For All Case status Most popular top 10 job posistions for H1b Visa Applications for ${var}"
       hadoop fs -rm -r /H1BVisaProject/Q5A_MostPopularTop10JobPositions
       hadoop jar /home/hduser/h1bvisa.jar com.niit.h1bvisa.Q5A_MostPopularTop10JobPositions /user/hive/warehouse/h1b.db/h1b_final /H1BVisaProject/Q5A_MostPopularTop10JobPositions $var;
       hadoop fs -cat /H1BVisaProject/Q5A_MostPopularTop10JobPositions/p*
       show_menu;

       ;;
     

       8) clear;
       option_picked "5 b) Find the most popular top 10 job positions for H1B visa applications for each year? For only certified applications.";
       echo -e "Enter the year (ALL | 2011,2012,2013,2014,2015,2016)"
       read var
       echo  "For only Certifiedd Case status Most popular top 10 job posistions for H1b Visa Applications for ${var}"
       hadoop fs -rm -r /H1BVisaProject/Q5B_MostPopularCertifiedTop10JobPositions
       hadoop jar h1bvisa.jar com.niit.h1bvisa.Q5B_MostPopularCertifiedTop10JobPositions /user/hive/warehouse/h1b.db/h1b_final /H1BVisaProject/Q5B_MostPopularCertifiedTop10JobPositions $var;
       hadoop fs -cat /H1BVisaProject/Q5B_MostPopularCertifiedTop10JobPositions/p*
       show_menu;

       ;;


       9) clear;
       option_picked "6) Find the percentage and the count of each case status on total applications for each year. Create a line graph depicting the pattern of All the cases over the period of time.";
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read var
       echo  "Number of Applications for  ${var}"
       pig -x local -p whichyear=${var} -f /home/hduser/Desktop/H1BVisaProject/Pig/Q6_PercenCountCaseStatusTotalAppYear.pig 
       show_menu;

       ;;


       10) clear;
       option_picked "7) Create a bar graph to depict the number of applications for each year. [All]";
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read var
       echo  "Number of Applications for  ${var}"
       pig -p whichyear=${var} -f /home/hduser/Desktop/H1BVisaProject/Pig/Q7_NumberOfApplicationsForEachYear.pig 
       show_menu;

       ;;


       11) clear;
       option_picked "8) Find the average Prevailing Wage for each Job for each Year (take part time and full time separate). Arrange the output in descending order - [Certified and Certified Withdrawn.]";
       echo -e "Enter the year (2011,2012,2013,2014,2015,2016)"
       read year
       echo -e "Enter the choice Full time / Part time. (Y/N)"
       read var
       echo  "Average Prevailing Wage for each Job for year ${year} and full time position ${var}"
       hive -e "SELECT year, case_status, job_title, full_time_position, AVG(prevailing_wage) AS avgprewage FROM h1b.h1b_final WHERE year = '$year' AND full_time_position = '$var'  AND case_status IN ('CERTIFIED','CERTIFIED-WITHDRAWN') GROUP BY year, case_status, job_title, full_time_position ORDER BY avgprewage DESC;";   
       show_menu;

       ;;
   
       12) clear;
       option_picked "9) Which are the employers along with the number of petitions who have the success rate more than 70%  in petitions. (total petitions filed 1000 OR more than 1000) ?";
       pig -x local /home/hduser/Desktop/H1BVisaProject/Pig/Q9_EmployersNoPetitionsSuccessRateMore70.pig 
       show_menu;

       ;;

  
       13) clear;
       option_picked "10) Which are the  job positions along with the number of petitions which have the success rate more than 70%  in petitions (total petitions filed 1000 OR more than 1000)?";
       hadoop fs -rm -r /H1BVisaProject/Q10_JobPositionsNoPetitionsSuccessRateAbove70Percent
       hadoop jar h1bvisa.jar com.niit.h1bvisa.Q10_JobPositionsNoPetitionsSuccessRateAbove70Percent /user/hive/warehouse/h1b.db/h1b_final /H1BVisaProject/Q10_JobPositionsNoPetitionsSuccessRateAbove70Percent
       hadoop fs -cat /H1BVisaProject/Q10_JobPositionsNoPetitionsSuccessRateAbove70Percent/p*
       show_menu;

       ;;

       
       14) clear;
       option_picked "11) Export result for question no 10 to MySql database."
       bash /home/hduser/Desktop/H1BVisaProject/Q11_ExportQ10AnsToMySqlDBUsingSqoop.sh
       show_menu;
      
       ;;



\n) exit;
        ;;

        *) clear;
        option_picked "Pick an option from the menu";
        show_menu;
        ;;
    esac
fi

done	

