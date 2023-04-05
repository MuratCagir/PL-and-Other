  drop public synonym pl;
  
  drop public synonym logtype;
  
  drop public synonym logs;
  
  create public synonym pl for etl_utl.pl;

  grant execute on etl_utl.logtype to public;

  grant execute on etl_utl.pl to public;
  
  create public synonym logtype for etl_utl.logtype;
 
  grant unlimited tablespace to etl_utl;
  
  grant all on etl_utl.logtype to public;
  
  create public synonym logs for etl_utl.logs;

--  select * from logs;
  
  commit;