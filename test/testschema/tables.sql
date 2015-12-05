create table table1
as select * from user_objects;

create table table2
as select * from user_tables;

create or replace trigger table1_trg1 before insert on table1 for each row
begin
  --table1_trg1
  null;
end;
/

create or replace trigger table1_trg2 before update on table2
begin
  null;
end;
/
