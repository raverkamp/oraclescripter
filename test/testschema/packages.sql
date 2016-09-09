create or replace package package1 as

procedure p1;

function p2 return number;

-- a comment

type rec is record (a integer,b varchar2(200));
type tab is table of rec;

function p3 return tab pipelined;

end;
/

create or replace package body package1 as

procedure p1 is
begin
 null;
end;

function p2 return number is
begin
  return null;
end;

function p3 return tab pipelined is
begin
  null;
end;

end;
/

create or replace package package2 as

procedure px;

function py return number;

end;
/

create or replace package body package2 as

procedure px is
begin
 null;
end;

function py return number is
begin
  return null;
end;

end;
/

create or replace package only_package_spec as

a integer;
end;
/