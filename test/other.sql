-- test for SqlPlus class, non code stuff
create sequence a;

   CREATE sequence x;

create synonym a 
for b;

create or   replace SYNONYM a
for B;

create or replace public    synonym a
for B;

  create public   synonym x for y;
create table a (x integer);
create index aaAa on b (c,d);

alter table bla add;
comment on table a is 'a';

comment on column a.b is 'a.b';

create procedure bla as
begin
 null;
end;
/
CREATE or REPLACE   procedure   bla   as
begin
 null   ;
end  bla;
/

create table a (x integer,
                y varchar2(200);

alter table bla add;

comment on table xyz is 'toll!';
comment on column xyz.abc is 'super ; ';

create index a on (b(u);

create unique index a on b(u);

