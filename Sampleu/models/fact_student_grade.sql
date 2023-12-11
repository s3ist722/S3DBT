WITH stg_studentgrade AS (
    SELECT 
        EnrollmentID,
        CourseID,
        StudentID,
        {{ dbt_utils.default__generate_surrogate_key(['StudentID']) }} AS StudentKey,
        {{ dbt_utils.default__generate_surrogate_key(['CourseID']) }} AS CourseKey,
        Grade
    FROM {{ source("sampleu", 'studentgrade') }}
),
stg_course AS (
    SELECT * FROM {{ source("sampleu", 'course') }}
),
course_credits AS (
    SELECT 
        sg.StudentID,
        sg.Grade,
        c.Credits,
        (sg.Grade * c.Credits) AS coursecredits
    FROM stg_studentgrade sg
    LEFT JOIN stg_course c ON sg.CourseID = c.CourseID
),
stg_GPA_calc AS (
    SELECT 
        cc.StudentID,
        ROUND(SUM(cc.coursecredits) / NULLIF(SUM(cc.Credits), 0), 2) AS GPA
    FROM 
        course_credits cc
    GROUP BY 
        cc.StudentID
),
Avg_Grade_Per_Course AS (
    SELECT 
        CourseID,
        AVG(Grade) AS class_avg
    FROM 
        stg_studentgrade
    GROUP BY 
        CourseID
)
SELECT 
    sg.EnrollmentID,
    sg.StudentKey,
    sg.StudentID,
    sg.CourseID,
    c.title as course_name,
    sg.CourseKey,
    gc.GPA,
    sg.Grade,
    ag.class_avg
FROM stg_studentgrade sg
LEFT JOIN stg_GPA_calc gc ON sg.StudentID = gc.StudentID
LEFT JOIN stg_course c ON sg.CourseID = c.CourseID
LEFT JOIN Avg_Grade_Per_Course ag ON sg.CourseID = ag.CourseID