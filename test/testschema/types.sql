create or replace type type1 as object
(
a integer,

member function bla return number

);
/

create or replace type body type1 as 


member function bla return number is
begin
  return 1;
end;

end;
/