with stg_instructor as (
    select * from {{source('sampleu', 'person')}} where Discriminator = 'Instructor'
),
stg_officeassignment as (
    select * from {{source("sampleu",'officeassignment')}}
)
select 
    {{dbt_utils.default__generate_surrogate_key(['i.personID'])}}
        as instructorkey,
    i.personID as instructorid,
    concat(i.LastName ,', ' , i.FirstName) as Instructor_name,
    i.HireDate,
    oa.location as instructor_office_loc,
    TRIM(SPLIT_PART(oa.location, ' ', 1)) AS instructor_office_No,
    CONCAT(TRIM(SPLIT_PART(oa.location, ' ', 2)), ' hall') AS instructor_office_building
from stg_instructor i
    left join stg_officeassignment oa on i.personID = oa.instructorid
where i.Discriminator = 'Instructor'