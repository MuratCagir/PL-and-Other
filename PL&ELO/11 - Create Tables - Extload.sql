drop table etl_utl.elo_tables;

create table etl_utl.elo_tables
(
  name                      varchar2(100) primary key,
  db_link                   varchar2(60),
  source                    varchar2(100),
  target                    varchar2(100),
  filter                    varchar2(4000),
  source_hint               varchar2(4000),
  target_hint               varchar2(4000),
  delta_column              varchar2(50),
  last_delta                varchar2(1000),
  excluded                  number default 0,
  drop_create               number default 0,
  create_options            varchar2(4000) default null,
  is_partitioned            number default 0,
  part_type                 varchar2(1) default null,
  part_init_diff            number default null, 
  part_final_diff           number default null,
  status                    varchar2(100) default 'READY',
  analyze                   number default 0,
  sql                       long,
  start_time                date,
  end_time                  date
)parallel
nologging
compress;



drop trigger etl_utl.trg_elo_tables_ucase;

create or replace trigger etl_utl.trg_elo_tables_ucase
   before insert
   on etl_utl.elo_tables
   for each row
declare
begin
   :new.name := upper (:new.name);
   :new.db_link := upper (:new.db_link);
   :new.source := :new.source;
   :new.target := upper (:new.target);
end;
/



drop table etl_utl.elo_columns;

create table etl_utl.elo_columns
(
   name         varchar2 (100),
   source_col   varchar2 (1000),
   target_col   varchar2 (50),
   excluded     number default 0,
   constraint pk_elo_columns primary key (name, target_col)
)
nologging;


drop trigger etl_utl.trg_elo_columns_ucase;

create or replace trigger etl_utl.trg_elo_columns_ucase
   before insert
   on etl_utl.elo_columns
   for each row
declare
begin
   :new.name := upper (:new.name);
   :new.source_col := :new.source_col;
   :new.target_col := upper (:new.target_col);
end;
/