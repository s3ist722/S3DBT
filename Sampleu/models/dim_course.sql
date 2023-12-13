with stg_course as (
    select * from {{ source("sampleu",'course') }}
),
stg_onsitecourse as (
    select * from {{ source("sampleu",'onsitecourse') }}
),
stg_onlinecourse as (
    select * from {{ source("sampleu",'onlinecourse') }}
)
select 
    {{ dbt_utils.default__generate_surrogate_key(['c.CourseID']) }} as Coursekey,
    c.CourseID,
    c.Title as Course_title,
    c.Credits as course_credit,
    c.DepartmentID,
    ol.URL as lecture_url,
    --os.location as lecture_loc,
    CASE
        WHEN os.location IS NULL THEN 'Online'
        ELSE CONCAT(os.location,' hall')
    END as lecture_loc,
    TRIM(SPLIT_PART(os.location, ' ', 1)) AS Lec_classroom_No,
    --CONCAT(TRIM(SPLIT_PART(os.location, ' ', 2)), ' hall') AS Lec_building,
    CASE
        WHEN os.location IS NULL THEN 'Online'
        ELSE CONCAT(TRIM(SPLIT_PART(os.location, ' ', 2)), ' hall')
    END AS Lec_building,
    os.Days as lec_days,
    CASE WHEN LEC_DAYS IS NULL THEN NULL WHEN LEC_DAYS LIKE '%M%' THEN 'YES' ELSE 'NO' END AS Monday,
    CASE WHEN LEC_DAYS IS NULL THEN NULL WHEN LEC_DAYS LIKE '%T%' THEN 'YES' ELSE 'NO' END AS Tuesday,
    CASE WHEN LEC_DAYS IS NULL THEN NULL WHEN LEC_DAYS LIKE '%W%' THEN 'YES' ELSE 'NO' END AS Wednesday,
    CASE WHEN LEC_DAYS IS NULL THEN NULL WHEN LEC_DAYS LIKE '%H%' THEN 'YES' ELSE 'NO' END AS Thursday,
    CASE WHEN LEC_DAYS IS NULL THEN NULL WHEN LEC_DAYS LIKE '%F%' THEN 'YES' ELSE 'NO' END AS Friday,
    os.Time as lec_time,
    CASE
        WHEN os.Time IS NULL THEN 'Online'
        WHEN os.Time < '10:00:00' THEN 'Early Morning'
        WHEN os.Time >= '10:00:00' AND os.Time < '12:00:00' THEN 'Morning'
        WHEN os.Time >= '12:00:00' AND os.Time < '16:00:00' THEN 'Afternoon'
        WHEN os.Time > '16:00:00' THEN 'Evening'
    END AS time_of_day
from stg_course c 
    left join stg_onlinecourse ol on ol.CourseID = c.CourseID
    left join stg_onsitecourse os on os.CourseID = c.CourseID