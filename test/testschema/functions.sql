create or replace function function1(x integer) return varchar2 is
begim
  return 'a';
end;

/
create or replace function function2(x integer) return number is
begim
  return 1;
end;

/
create or replace function function3(x integer) return date is
begim
  return sysdate;
end;

/

