with stg_course AS (
    SELECT 
        *,
        {{ dbt_utils.default__generate_surrogate_key(['CourseID']) }} AS CourseKey
    FROM {{ source("sampleu", 'course') }}
),
stg_Course_Evaluation as (
    select * from {{source('sampleu','courseevaluations')}}
),
AverageRatings AS (
    SELECT 
        CourseID, 
        AVG(Rating) AS AVG_RATING,
        count(Rating) as rating_count
    FROM 
        stg_Course_Evaluation 
    GROUP BY CourseID
)
SELECT 
    c.CourseID,
    c.CourseKey, 
    ce.Rating, 
    ce.Evaluation,
    ar.AVG_RATING,
    ar.rating_count
FROM 
    stg_Course_Evaluation ce
left join AverageRatings ar ON ce.COURSEID = ar.COURSEID
left join stg_course c on ce.CourseID = c.CourseID
