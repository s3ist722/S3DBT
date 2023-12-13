with stg_CourseInstructor as (
    select 
        CourseID,
        PersonID as InstructorID,
        {{ dbt_utils.default__generate_surrogate_key(['PersonID']) }} AS InstructorKey,
        {{ dbt_utils.default__generate_surrogate_key(['CourseID']) }} AS CourseKey
    from {{source('sampleu','courseinstructor')}} 
),
stg_course AS (
    SELECT * FROM {{ source("sampleu", 'course') }}
),
stg_Department as (
    select * from {{source('sampleu','department')}}
)
select 
    ci.*,
    {{ dbt_utils.default__generate_surrogate_key(['d.DepartmentID']) }} AS departmentkey
from stg_CourseInstructor ci 
    LEFT JOIN stg_course c ON ci.CourseID = c.CourseID
    left join stg_Department d on d.DepartmentID = c.DepartmentID
    

    