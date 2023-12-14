IST 722 - DATA WAREHOUSE

# Group S3 SampleU Project

## Overview

Our project aimed to gain a deep insight into university operations through a data-driven approach. We explored online and offline courses, profiles of instructors and students, departmental structures, and external data from S3 containing course ratings and reviews. Choosing Snowflake as our data warehousing platform, we laid the foundation for a modern data management system.

## Data Extraction and Loading (ELT Process)

Our primary focus was curating data from various sources, including course details, instructor and student profiles, departmental hierarchies, and external feedback from S3. Snowflake facilitated a streamlined Extraction and load process, the result was stagged raw data in snowflake for further processing.

## Transformations and Analytics (dbt)

Within dbt, raw data underwent transformative processes, introducing necessary columns and intricate transformations. We calculated average class grade, GPA with the help of credits of a course and grade of student. We also categorized the lecture time column such that it will tell us the time of the day lecture is in(early morning, morning, afternoon). We combined online and onsite courses into the dim_course and introduced the lecture_session column which will define the time of the day for classes as stated above. After that we separated students and instructors from person table creating two different dimensions for easy and efficient access in future. This refined dataset was prepared for advanced analytics and migrated into analytics database for exploration and powerBI report generation.

Lineages for our defined business processes -

![image](https://github.com/s3ist722/S3DBT/blob/main/images/fact_assigned_instructor.png)
![image](https://github.com/s3ist722/S3DBT/blob/main/images/fact_course_evaluation.png)  
![image](https://github.com/s3ist722/S3DBT/blob/main/images/fact_student_grade.png)

## Visualization and Analysis (Power BI)

The synergy between dbt and Power BI marked the visualization and analysis phase. The integration allowed us to translate meticulously prepared data into insightful visualizations. For instance, bar graphs on course demographics page provided insights into total course ratings and average ratings of the courses, and lecture sessions. Student performance analysis delved into subject-wise grades and overall academic GPA, linked to individual student IDs. Notably, personalized visualizations for each student illustrated their chosen courses, corresponding GPAs, and course schedules. This individualized perspective not only aids in monitoring academic progress but also enables the design of targeted support and intervention strategies based on each student's unique journey. Such dashboard would be very effective for college academic counselors who can see all the details about student just by selecting student name or ID.

![image](https://github.com/s3ist722/S3DBT/blob/main/images/Course%20Demo.png)
![image](https://github.com/s3ist722/S3DBT/blob/main/images/Student%20Demo.png)  
![image](https://github.com/s3ist722/S3DBT/blob/main/images/Department%20Details.png)

## Contributors

Sahil Wani, Satwik Belaldavar, Shruti Rao
