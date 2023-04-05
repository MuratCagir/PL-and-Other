  drop table etl_utl.params;
  
  create table etl_utl.params (
    name  varchar2(100)   primary key,
    value varchar2(4000),
    description varchar2(4000)
  );

  drop trigger etl_utl.trg_logs;

  drop sequence etl_utl.seq_logs;

  drop table etl_utl.logs;

  create table etl_utl.logs (
    id          number,
    name        varchar2(1000),
    log_level   varchar2(20),
    start_date  date,
    end_date    date,
    message     varchar2(4000),
    statement   clob
  ) parallel nologging compress
  ;

  create unique index etl_utl.pk_etl_utl_logs on etl_utl.logs
  (id)
  nologging
  storage  (
            buffer_pool      default
            flash_cache      default
            cell_flash_cache default
           )
  parallel;

  alter table etl_utl.logs
  add constraint pk_etl_utl_logs
  primary key (id);
  
  create sequence etl_utl.seq_logs start with 1;

  create or replace trigger etl_utl.trg_logs 
    before insert on etl_utl.logs 
    for each row
    begin
      select etl_utl.seq_logs.nextval
      into   :new.id
      from   dual;
    end;