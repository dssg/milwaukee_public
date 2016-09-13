  -- create baseline table for 2013 
  
  drop table if exists training.baseline_older_students ;
  create table training.baseline_older_students as (
  select person_id, a.student_key 
  from training.mapping a
  join edu_schema.new_demographic c on a.student_key= c.student_key and student_grade_code in ('01','02','03','04','05','06','07','08')
  join edu_schema.discipline b on a.student_key = b.student_key 
  and extract(year from discipline_start_date)= 2013 
  group by 1,2 
  )
  
  -- query in python script 
/*   SELECT person_id, a.student_key, discipline_start_date as discipline_date, 
             --CASE when student_grade_code IN ('01','02','03','04','05','06','07','08') then 1 else end 0 as first_grades, 
             CASE when student_grade_code IN ('09','10','11','12') then 1 else 0 end as last_grades, 
             CASE WHEN discipline_state_action_group like '%Suspension%' then 1 else 0 end as suspension
             FROM edu_schema.discipline a
             RIGHT JOIN training.mapping b ON a.student_key=b.student_key and extract(year from discipline_start_date) = 2013 
             JOIN edu_schema.new_demographic c on b.student_key=c.student_key and extract(year from collection_date) = 2012
             */ 
          
  -- table baseline2 was created by python script baseline.py
  drop table if exists training.baseline_all_students;
  create table training.baseline_all_students as (
  select student_key from training.baseline_older_students
  union 
  select labels from training.baseline_younger_students
  group by 1) 
  
  
  create table training.baseline_results as (
  select case when a.student_key is not null then 1 else 0 end as baseline_tag, m.person_id, year_1314 from training.baseline_all_students a
  right join training.mapping m using (student_key)
  join training.labels s on s.person_id = m.person_id
  );
  
--  calculate baseline precision and recall 
-- filter out as the training-set (the ones that already HAD intercation with the CJ system) 

create table training.baseline_calculation as (
select *, 
case when baseline_tag = 1 and year_1314 = 1 then 1 else 0 end as TP, --1842
case when baseline_tag = 0 and year_1314 = 1 then 1 else 0 end as FN, --1903
case when baseline_tag = 0 and year_1314 = 0 then 1 else 0 end as TN, --277586
case when baseline_tag = 1 and year_1314 = 0 then 1 else 0 end as FP -- 23344 
join training.features2013 using (person_id)
from training.baseline_results); 




select sum(TP), sum(TN), sum(FN), sum(FP)
from training.baseline_calculation;

-- check overlapping between the models 
select 
count(model_person_id), 
count(baseline_person_id), 
sum(year_1415),
sum(case when model_person_id is null and baseline_person_id is not null and year_1415  = 1 then 1 else 0 end) as only_baseline ,
sum(case when baseline_person_id is null and model_person_id is not null and year_1415  = 1 then 1 else 0 end) as only_model, 
sum(case when model_person_id is not null and year_1415 =1 then 1 else 0 end) as model_hit, 
sum(case when baseline_person_id is not null and year_1415 =1 then 1 else 0 end) as baseline_hit ,
sum(case when model_person_id  is not null and baseline_person_id is not null then 1 else 0 end) as both_flag ,
sum(case when model_person_id  is not null and baseline_person_id is not null  and year_1415 = 1 then 1 else 0 end) as both_hit
from training.overlapping_baseline_model_2013;




-- check precision and recall in specific k which match the baseline--- stats for poster
select count(*), sum(year_1415)
from training.model4513 
join training.labels using (person_id) 
where risk_score > 0.0238;


-- check precision and recall in specific k which match the baseline--- stats for poster
select count(*), sum(year_1415)
from training.model4513 
join training.labels using (person_id) 
where risk_score > 0.07;











             
           
