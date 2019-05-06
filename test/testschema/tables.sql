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
 u varchar2(20 byte),
 v varchar2(20 char),
 w varchar2(20),
 a timestamp with local time zone,
 b timestamp with time zone,
 c timestamp(6) with time zone,
 d timestamp(4) with time zone,
 e timestamp);
 
 alter table table3 add constraint pk_table3  primary key(x,y);
 
 alter table table3 add constraint uk_table3  unique (v,w);
 alter table table3 add constraint aauk_table3  unique (u,w);
 
 create table table3b
 (u number,
 x0 number,
 y0 number,
 constraint pk_table3b primary key (u),
 constraint fk_table3b foreign key (x0,y0) references table3(x,y),
 constraint fk_table3b_rev foreign key (y0,x0) references table3(x,y));

  

  CREATE GLOBAL TEMPORARY TABLE table4
   (	X NUMBER, 
	V VARCHAR2(200 BYTE)) ON COMMIT DELETE ROWS ;
   
   
  CREATE GLOBAL TEMPORARY TABLE TABLE5
   (X NUMBER, 
	V VARCHAR2(200 BYTE)
   ) ON COMMIT PRESERVE ROWS ;

  CREATE GLOBAL TEMPORARY TABLE TABLE6
   (X NUMBER, 
	  V VARCHAR2 (200 BYTE)) ;
    

create table table7 
( x integer, 
  z number,
  constraint x check (x>0),
  constraint fk_table7 foreign key (x) references table3b(u));

 
 