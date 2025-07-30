
    
    

select
    id as unique_field,
    count(*) as n_records

from "spacex_db"."bronze"."launches"
where id is not null
group by id
having count(*) > 1


