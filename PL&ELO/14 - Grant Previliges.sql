  drop public synonym elo;
  
  create public synonym elo for etl_utl.elo;

  grant execute on etl_utl.elo to public;