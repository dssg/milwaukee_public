CREATE TABLE cj_schema.juv_case_person_id as 
  (SELECT a.*, b.pid from cj_schema.juv_case_complete_id a
    JOIN training.mapping b on a.person_id = b.person_id)
with data;
