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

create table table3 
(x number(2),
 y number(3,4),
 a timestamp with local time zone,
 b timestamp with time zone,
 c timestamp(6) with time zone,
 d timestamp(4) with time zone
 );
 