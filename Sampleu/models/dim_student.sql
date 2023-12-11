with stg_student as (
    select * from {{source('sampleu', 'person')}} where discriminator = 'Student'
)
select 
    {{dbt_utils.default__generate_surrogate_key(['s.personID'])}}
        as studentkey,
    s.personID as studentid,
    concat(s.LastName ,', ' , s.FirstName) as fullname,
    s.EnrollmentDate
from stg_student s