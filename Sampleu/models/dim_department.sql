with stg_department as (
    select * from {{source('sampleu','department')}}
)
select 
    {{dbt_utils.default__generate_surrogate_key(['d.DepartmentID'])}}
        as departmentkey,
    d.DepartmentID,
    d.name as department_name,
    d.Budget,
    d.Administrator
from stg_department d 

