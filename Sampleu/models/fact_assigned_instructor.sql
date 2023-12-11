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
),
stg_instructor as (
    select * from {{source('sampleu', 'person')}} where Discriminator = 'Instructor'
)
select 
    ci.*,
    c.Title as Course_name,
    {{ dbt_utils.default__generate_surrogate_key(['d.DepartmentID']) }} AS departmentkey,
    d.name as department_name,
    concat(i.LastName ,', ' , i.FirstName) as Instructor_name,
    i.HireDate
from stg_CourseInstructor ci 
    LEFT JOIN stg_course c ON ci.CourseID = c.CourseID
    left join stg_Department d on d.DepartmentID = c.DepartmentID
    left join stg_instructor i on i.personID = ci. InstructorID

    