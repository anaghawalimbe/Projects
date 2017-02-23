drop table top_racers;
create table top_racers
(
id string,
finalPlace int,
hometown string,
division string,
time string
)
row format
delimited fields terminated by '\t'
stored as TEXTFILE
location '/user/awalimbe/case/top_racers'
tblproperties("skip.header.line.count"="1");

----change the location to run this code----------------
load data local inpath '/home/awalimbe/case/racers_tab.txt' into table top_racers;





--/********Step 1: Creating new fields for wave,gender,age,city,state,mins,secs,total minutes********/

drop table top_racers_clean_step1;

create table top_racers_clean_step1
row format
delimited fields terminated by '\t'
stored as TEXTFILE
location '/user/awalimbe/case/top_racers_clean_step1'

as

  select *, 
         --Extracting wave from id
         regexp_extract(upper(id),'[A-Z]*',0) as wave, 
         
         --Extracting gender from division
         regexp_extract(upper(division),'[A-Z]*',0) as gender,
         
         --Extracting age from division
         cast(regexp_extract(upper(division),'\\d+',0) as int) as age,
         
         --Extracting minutes from time
         cast(substr(regexp_replace(time,'</td',''),1,instr(time,':')-1) as int) as mins,
         
         --Extracting secs from time
         cast(substr(regexp_replace(time,'</td',''),instr(time,':')+1) as double) as secs,
         
         --Extracting city from hometown
         UPPER(trim(substr(hometown,1,instr(hometown,',')-1))) as city,
         
         --Extracting State from hometown
         UPPER(trim(substr(hometown,instr(hometown,',')+1))) as state
         
         
            
         
  from top_racers 
;





---***********************STEP2**************************************************************

drop table top_racers_clean_step2;

create table top_racers_clean_step2
row format
delimited fields terminated by '\t'
stored as TEXTFILE
location '/user/awalimbe/case/top_racers_clean_step2'

as

select *, 
       
       dense_rank() over (order by mins, secs) as rank,
       (10/(total_minutes/60)) as speed

from

    (select
         id,
         finalplace,
         hometown,
         division,
         time,
         wave,
         case when gender='M' then 'Male' when gender='F' then 'Female' else 'Not Available' end as gender,
         age,
         mins,
         secs,
         mins+(secs/60) as total_minutes,
         city,
         --cleaning state--
         case when length(state)<1 then 'Not Available' else trim(regexp_replace(state,',','')) end as state
      from  top_racers_clean_step1
    ---deleting record with displaced data and no time record
      where mins is not null
    ) as clean_table
;


---select wave, count(*) as c from top_racers_clean_step1 group by wave order by c desc ;
---select gender, count(*) as c from top_racers_clean_step1 group by gender order by c desc;




