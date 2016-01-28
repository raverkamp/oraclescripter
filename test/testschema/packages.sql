create or replace package package1 as

procedure p1;

function p2 return number;

-- a comment

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