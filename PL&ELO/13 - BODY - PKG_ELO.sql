CREATE OR REPLACE PACKAGE BODY ETL_UTL.ELO
as


  gv_error    constant varchar2(10) := 'ERROR';
  gv_success  constant varchar2(10) := 'SUCCESS';
  gv_running  constant varchar2(10) := 'RUNNING';
  gv_ready    constant varchar2(10) := 'READY';

  gv_job_module   varchar2(50)  := 'ELO';                 -- Job Module Name : Extract Load package
  gv_pck          varchar2(50)  := 'ELO';                 -- PLSQL Package Name
  gv_job_owner    varchar2(50)  := 'ETL_UTL';             -- Owner of the Job
  gv_proc         varchar2(100);                          -- Procedure Name
  gv_delim        varchar2(10)  := ' : ';                 -- Delimiter Used In Logging

  gv_sql_errm     varchar2(4000);                         -- SQL Error Message
  gv_sql_errc     number;                                 -- SQL Error Code
  gv_sql          long := '';
  gv_date_format  varchar2(30) := 'yyyy.mm.dd hh24:mi:ss';

  function build_sql(i_name varchar2) return clob;

  function fun_get_delta_col_type(
    i_db_link varchar2,
    i_table   varchar2,
    i_column  varchar2) return varchar2;

  procedure prc_update_last_delta(
    i_name            varchar2,
    i_table           varchar2,
    i_delta_col       varchar2,
    i_delta_col_type  varchar2
  );

  procedure set_status(i_name varchar2, i_status varchar2) as
    v_status varchar2(100);
    status_change_error exception;
    already_running_error exception;
    pragma exception_init(status_change_error, -20001);
    pragma exception_init(already_running_error, -20002);
  begin

  select t.status into v_status from etl_utl.elo_tables t where upper(t.name) = upper(i_name);

    if upper(i_status) = 'RUNNING' and v_status = 'RUNNING' then
      raise_application_error(-20002, i_name || ' is already running!');
    end if;
    update etl_utl.elo_tables set
      status = upper(i_status),
      start_time = decode(upper(i_status), 'RUNNING', sysdate, start_time),
      end_time = case when upper(i_status) in ('ERROR', 'SUCCESS') then sysdate else end_time end
    where upper(name) = upper(i_name);

    commit;
  end;

  function is_excluded(i_name varchar2) return boolean
  is
    v_count number := 0;
  begin
    select count(1) into v_count from etl_utl.elo_tables t
    where
      upper(t.name) = upper(i_name) and
      t.excluded = 1;
    return case v_count when 0 then false else true end;
  end;


  procedure reset is
  begin
    gv_proc := 'RESET';
    pl.logger := etl_utl.logtype.init(gv_pck||'.'||gv_proc);
    gv_sql := 'update etl_utl.elo_tables set
      start_time = null,
      end_time = null,
      status = upper('''|| gv_ready || ''')
    ';
    execute immediate gv_sql;
    pl.logger.success(sql%rowcount || ' : updated', gv_sql);
    commit;
  exception
    when others then
      pl.logger.error(sqlerrm, gv_sql);
      raise;
  end;


  procedure run(i_name varchar2, i_drop_create number default null, i_analyze number default null, i_index_drop number := 1)
  is
    v_db_link               varchar2(60);
    v_source                varchar2(100);
    v_target                varchar2(100);
    v_filter                varchar2(4000);
    v_source_hint           varchar2(4000);
    v_target_hint           varchar2(4000);
    v_analyze               number;
    v_drop_create           number;
    v_create_options        varchar2(4000);
    v_is_partitioned        number;
    v_part_type             varchar2(1);
    v_part_init_diff        number;
    v_part_final_diff       number;
    v_partition             varchar2(100);
    v_delta_column          varchar2(50);
    v_last_delta            varchar2(1000);
    v_source_cols           long;
    v_target_cols           long;
    v_delta_data_type       varchar2(50);
    v_sql                   long;
    v_index_ddls            dbms_sql.varchar2_table;
    v_index_created         boolean := false;
    v_sql_loop              long;
  begin

    gv_proc := 'RUN';

    -- Initialize Log Variables
    pl.logger := etl_utl.logtype.init(gv_pck||'.'||gv_proc||'-'||i_name);

    if is_excluded(i_name) = true then
      pl.logger.warning(i_name || ' is EXCLUDED!');
      goto end_proc;
    end if;

    set_status(i_name, gv_running);

    pl.logger.info('Started extraction ...');

    select
      db_link, source, target, filter, source_hint, target_hint, v_delta_column, last_delta,
      drop_create, create_options, is_partitioned, part_type, part_init_diff, part_final_diff, analyze, sql
    into
      v_db_link, v_source, v_target, v_filter, v_source_hint, v_target_hint, v_delta_column, v_last_delta,
      v_drop_create, v_create_options, v_is_partitioned, v_part_type, v_part_init_diff, v_part_final_diff, v_analyze, v_sql
    from etl_utl.elo_tables where name = i_name;

    if trim(v_target_hint) is not null and instr(v_target_hint,'/*+') = 0
    then
      v_target_hint := '/*+ '||v_target_hint||' */';
    end if;

    if trim(v_source_hint) is not null and instr(v_source_hint,'/*+') = 0
    then
      v_source_hint := '/*+ '||v_source_hint||' */';
    end if;

    if v_delta_column is not null then
      v_delta_data_type := fun_get_delta_col_type(v_db_link, v_source, v_delta_column);
    end if;

    if v_filter is not null or v_delta_column is not null then
      v_filter := 'WHERE ' || v_filter;
      if v_delta_column is not null then
        v_filter := v_filter || ' AND '||v_delta_column||'>'||
        case v_delta_data_type
          when 'DATE'     then pl.date_string(to_date(v_last_delta,gv_date_format))
          when 'NUMBER'   then to_number(v_last_delta)
          when 'CHAR'     then v_last_delta
          when 'VARCHAR'  then v_last_delta
          else v_last_delta
        end;
      end if;
    end if;

    begin
      for c in (select source_col, target_col from etl_utl.elo_columns where excluded = 0 and name = i_name) loop
        v_source_cols := v_source_cols || c.source_col || ',';
        v_target_cols := v_target_cols || c.target_col || ',';
      end loop;

      v_source_cols := rtrim(v_source_cols, ',');
      v_target_cols := rtrim(v_target_cols, ',');
    exception when no_data_found then
      pl.logger.warning(i_name || ': All columns excluded or no elo def!');
      goto end_proc;
    end;

    if v_is_partitioned != 1 then
      v_filter := replace(upper(v_filter), upper('v_part_init_diff'), to_char(v_part_init_diff));
      v_filter := replace(upper(v_filter), upper('v_part_final_diff'), to_char(v_part_final_diff));
    end if;

    if (i_drop_create is not null and i_drop_create != 0) or (i_drop_create is null and v_drop_create != 0) then

      pl.drop_table(v_target);

      if v_sql is null then
        gv_sql := '
          CREATE TABLE '|| v_target ||'
          '||v_create_options||'
          ('||v_target_cols|| ')
          AS
          SELECT '||v_source_hint||'
          '||v_source_cols||'
          FROM
            '||v_source||'@'||v_db_link||'
            '||v_filter||'';
      else
        gv_sql := v_sql;
      end if;
        
    else
      if v_sql is null then
        if v_is_partitioned != 1 then
          gv_sql := '
          INSERT '||v_target_hint||' INTO '|| v_target ||'
          ('||v_target_cols|| ')
          SELECT '||v_source_hint||'
          '||v_source_cols||'
          FROM
              '||v_source||'@'||v_db_link||'
              '||v_filter||'';
        else
          gv_sql := '
          INSERT '||v_target_hint||' INTO '|| v_target ||' PARTITION (v_partition)
          ('||v_target_cols|| ')
          SELECT '||v_source_hint||'
          '||v_source_cols||'
          FROM
              '||v_source||'@'||v_db_link||'
              '||v_filter||'';
        end if;
      else
        gv_sql := v_sql;
      end if;

      if v_delta_column is null and v_is_partitioned != 1 then
        pl.logger.info('Started truncating the table ' || v_target || ' ...');
        execute immediate 'truncate table ' || v_target;
        pl.logger.success('Truncated the table ' || v_target, 'truncate table ' || v_target);
      end if;

      if v_is_partitioned = 1 then
        if v_part_type = 'D' then
          pl.add_partitions(i_owner => substr(v_target, 1, instr(v_target, '.')-1), i_table => substr(v_target, instr(v_target, '.')+1, 30), i_date => sysdate+1);
        elsif v_part_type = 'M' then
          pl.add_partitions(i_owner => substr(v_target, 1, instr(v_target, '.')-1), i_table => substr(v_target, instr(v_target, '.')+1, 30), i_date => add_months(sysdate,1));
        end if;
        for i in v_part_final_diff..v_part_init_diff
        loop
          if v_part_type = 'D' then
            v_partition := 'P'||to_char(sysdate-(i),'yyyymmdd');
          elsif v_part_type = 'M' then
            v_partition := 'P'||to_char(add_months(sysdate,-(i)),'yyyymm');
          end if;
          pl.truncate_partition(i_owner => substr(v_target, 1, instr(v_target, '.')-1), i_table => substr(v_target, instr(v_target, '.')+1, 30), i_partition => v_partition);
        end loop;
      end if;
      
    end if;


    if i_index_drop != 0 then
      v_index_ddls := pl.index_ddls(v_target);
      pl.drop_indexes(v_target);
    end if;

    if v_is_partitioned != 1 then
      pl.logger.info('Started row inserting ...', gv_sql);
      execute immediate 'alter session enable parallel dml';
      execute immediate gv_sql;
      pl.logger.success(sql%rowcount || ' rows inserted', gv_sql);
      commit;
    else
      for i in v_part_final_diff..v_part_init_diff
      loop
        v_sql_loop := gv_sql;
        if v_part_type = 'D' then
          v_partition := 'P'||to_char(sysdate-(i),'yyyymmdd');
        elsif v_part_type = 'M' then
          v_partition := 'P'||to_char(add_months(sysdate,-(i)),'yyyymm');
        end if;
        
        v_sql_loop := replace(upper(v_sql_loop), upper('v_partition'), v_partition);
        v_sql_loop := replace(upper(v_sql_loop), upper('v_part_init_diff'), to_char(i));
        v_sql_loop := replace(upper(v_sql_loop), upper('v_part_final_diff'), to_char(i));

        pl.logger.info('Started row inserting to the partition '||v_partition||'...', v_sql_loop);
        execute immediate 'alter session enable parallel dml';
        execute immediate v_sql_loop;
        pl.logger.success(sql%rowcount || ' rows inserted to the partition '||v_partition, v_sql_loop);
        commit;
      end loop;
    end if;

    if i_index_drop != 0 then
      pl.exec(v_index_ddls, true);
      v_index_created := true;
    end if;

    if v_delta_column is not null then
      prc_update_last_delta(
        i_name  => i_name,
        i_table => v_target,
        i_delta_col => v_delta_column,
        i_delta_col_type => v_delta_data_type
      );
    end if;

    if ((i_analyze is not null and i_analyze != 0) or (v_analyze != 0)) and v_is_partitioned != 1 then
      pl.logger.info('Started table stats gathering ...');
      pl.gather_table_stats(i_table => v_target);
      pl.logger.success(v_target||' table stats gathered');
      commit;
    elsif ((i_analyze is not null and i_analyze != 0) or (v_analyze != 0)) and v_is_partitioned = 1 then
      for i in v_part_final_diff..v_part_init_diff
        loop
          if v_part_type = 'D' then
            v_partition := 'P'||to_char(sysdate-(i),'yyyymmdd');
          elsif v_part_type = 'M' then
            v_partition := 'P'||to_char(add_months(sysdate,-(i)),'yyyymm');
          end if;
          pl.logger.info('Started analyzing the partition '||v_partition||'...');
          dbms_stats.gather_table_stats(ownname => substr(v_target, 1, instr(v_target, '.')-1), tabname => substr(v_target, instr(v_target, '.')+1, 30), partname => v_partition, degree=> 4, cascade=> true);
          pl.logger.success('Analyzed the partition '||v_partition);
        end loop;
      commit;
    end if;
    
    pl.logger.info('The procedure ended OK');
    <<end_proc>>
    set_status(i_name, gv_success);
  exception
    when others then
      pl.logger.error(sqlerrm, gv_sql);
      set_status(i_name, gv_error);
      if i_index_drop != 0 and v_index_created = false then
        pl.exec(v_index_ddls, true);
      end if;
      raise;
  end;

  function fun_get_delta_col_type(i_db_link varchar2, i_table varchar2, i_column varchar2) return varchar2
  is
    v_owner       varchar2(30) := substr(i_table,1,instr(i_table, '.')-1);
    v_table_name  varchar2(30) := substr(i_table,instr(i_table, '.')+1);
    v_col_type    varchar2(50);
  begin

    gv_sql:= '
      select data_type from all_tab_cols@'||i_db_link||'
      where owner = '''||v_owner||''' and table_name='''||v_table_name||''' and column_name = '''|| i_column||'''
    ';

    execute immediate gv_sql into v_col_type;

    return gv_sql;

  end;

  procedure prc_update_last_delta(
    i_name            varchar2,
    i_table           varchar2,
    i_delta_col       varchar2,
    i_delta_col_type  varchar2
  )
  is
    v_last_delta varchar2(1000);
  begin

    if i_delta_col_type = 'DATE' then
      gv_sql := 'to_char(max('||i_delta_col||'),'''||gv_date_format||''')';
    else
      gv_sql := 'max('||i_delta_col||')';
    end if;

    gv_sql := 'select /*+ parallel(16) */ '||gv_sql||' from '||i_table;

    execute immediate gv_sql into v_last_delta;

    gv_sql := '
      update ETL_UTL.ELO_TABLES set last_delta = '''||v_last_delta||'''
      where name = '''||i_name||'''
    ';

    execute immediate gv_sql;

    commit;
  end;


  -- Args:
  --    [i_source varchar2]: 'owner.source_table@dblink'
  --    [i_create boolean := false]: create target table
  procedure def(i_source varchar2, i_create boolean := false) is
    v_tokens dbms_sql.varchar2_table;
  begin
    v_tokens := pl.split(i_source, '@');
    def(
      i_source  => v_tokens(1),
      i_dblk    => v_tokens(2),
      i_create  => i_create
    );
  end;

  -- sugar for define
  procedure def(
    i_source varchar2,
    i_dblk  varchar2,
    i_elo_name  varchar2 default null,
    i_target_schema varchar2 default 'SG',
    i_create  boolean default false
  ) as
  begin
    define(
      i_source => i_source,
      i_dblk  => i_dblk,
      i_elo_name => i_elo_name,
      i_target_schema => i_target_schema,
      i_create => i_create
    );
  end;

  procedure define(
    i_source varchar2,
    i_dblk  varchar2,
    i_elo_name  varchar2 default null,
    i_target_schema varchar2 default 'SG',
    i_create  boolean default false
  )
  is
    table_is_null     exception;
    db_link_is_null   exception;

    pragma exception_init(table_is_null,   -20170);
    pragma exception_init(db_link_is_null, -20171);

    v_script  long := '';
    v_columns long := '';

    type source_cursor_type is ref cursor;
    c source_cursor_type;

    v_column_name  varchar2(100);
    v_data_type    varchar2(100);
    v_data_length  number;
  begin

    gv_proc := 'DEFINE';

    -- Initialize Log Variables
    pl.logger := etl_utl.logtype.init(gv_pck || '.' || gv_proc);

    if i_source is null then raise table_is_null; end if;

    if i_target_schema is null then raise db_link_is_null; end if;

    gv_sql := 'select column_name, data_type, data_length from all_tab_cols@'||i_dblk|| '
      where owner||''.''||table_name = '''||upper(i_source)||''' and
      hidden_column = ''NO''
    ';

    if i_create = true then
      pl.drop_table(i_target_schema||'.'||substr(i_source, 1 + instr(i_source,'.')));
      v_script := '
        create table '||i_target_schema||'.'||substr(i_source, 1 + instr(i_source,'.'))||'
        (
          $COLUMNS
        )
      ';

      open c for gv_sql;
      loop
        fetch c into v_column_name, v_data_type, v_data_length;
        exit when c%notfound;
        if v_data_type in ('CHAR','VARCHAR','VARCHAR2','NUMBER', 'RAW') then
          v_columns := v_columns || v_column_name||' '||v_data_type||'('||v_data_length||'),'||chr(10);
        else
          v_columns := v_columns || v_column_name||' '||v_data_type||','||chr(10);
        end if;
      end loop;

      v_columns := rtrim(v_columns, ','||chr(10));
      v_script := replace(v_script,'$COLUMNS',v_columns) ||chr(10)||chr(10);

      execute immediate v_script;
      pl.logger.success('Table created', v_script);
    end if;

    -- define extraction table
    v_script := 'INSERT INTO ETL_UTL.ELO_TABLES (
      NAME,
      DB_LINK,
      SOURCE,
      TARGET,
      SOURCE_HINT,  
      TARGET_HINT,
      ANALYZE
    ) VALUES (
      '''||nvl(i_elo_name, i_source)||''',
      '''||i_dblk||''',
      '''||i_source||''',
      '''||i_target_schema||'.'||substr(i_source, 1 + instr(i_source,'.'))||''',
      ''parallel(16)'',
      ''append nologging'',
      1
    )';

    execute immediate v_script;
    pl.logger.success('Table defined in elo_tables', v_script);


    -- define extraction rules
    open c for gv_sql;
    loop
      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      v_script := 'INSERT INTO ETL_UTL.ELO_COLUMNS (
        NAME,
        SOURCE_COL,
        TARGET_COL
      ) VALUES (
        '''||nvl(i_elo_name, i_source)||''',
        '''||v_column_name||''',
        '''||v_column_name||'''
      )';

      execute immediate v_script;
      pl.logger.success('Column defined in elo_columns', v_script);
    end loop;

    commit;

  exception
    when others then
      gv_sql_errc := sqlcode;
      gv_sql_errm := sqlerrm;
      pl.logger.error(gv_sql_errc||' : '||gv_sql_errm, gv_sql);
      raise;
  end;

  function script(
    i_table varchar2,
    i_dblk  varchar2,
    i_name  varchar2 default null,
    i_target_schema varchar2 default 'SG'
  ) return varchar2
  is
    table_is_null     exception;
    db_link_is_null   exception;

    pragma exception_init(table_is_null,   -20170);
    pragma exception_init(db_link_is_null, -20171);

    v_script  long := '';
    v_columns long := '';

    type source_cursor_type is ref cursor;
    c source_cursor_type;

     v_column_name  varchar2(100);
     v_data_type    varchar2(100);
     v_data_length  number;
  begin

    if i_table is null then raise table_is_null; end if;

    if i_target_schema is null then raise db_link_is_null; end if;


    v_script := '
      create table '||i_target_schema||'.'||substr(i_table, instr(i_target_schema,'.'))||'
      (
        $COLUMNS
      );
    ';

    gv_sql := 'select column_name, data_type, data_length from all_tab_cols@'||i_dblk|| '
      where owner||''.''||table_name = '''||upper(i_table)||''' and
      hidden_column = ''NO''
    ';
    open c for gv_sql;

    loop

      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      if v_data_type in ('CHAR','VARCHAR','VARCHAR2','NUMBER', 'RAW') then
        v_columns := v_columns || v_column_name||' '||v_data_type||'('||v_data_length||'),'||chr(10);
      else
        v_columns := v_columns || v_column_name||' '||v_data_type||','||chr(10);
      end if;
    end loop;

    v_columns := rtrim(v_columns, ','||chr(10));

    v_script := replace(v_script,'$COLUMNS',v_columns) ||chr(10)||chr(10);

    v_script := v_script || 'INSERT INTO ETL_UTL.ELO_TABLES (
      NAME,
      DB_LINK,
      SOURCE,
      TARGET
    ) VALUES (
      '''||nvl(i_name,i_table)||''',
      '''||i_dblk||''',
      '''||i_table||''',
      '''||i_target_schema||'.'||substr(i_table, instr(i_target_schema,'.'))||'''
    );'||chr(10)||chr(10);


    open c for gv_sql;

    loop

      fetch c into v_column_name, v_data_type, v_data_length;
      exit when c%notfound;

      v_script := v_script || 'INSERT INTO ETL_UTL.ELO_COLUMNS (
        NAME,
        SOURCE_COL,
        TARGET_COL
      ) VALUES (
        '''||nvl(i_name, i_table)||''',
        '''||v_column_name||''',
        '''||v_column_name||'''
      );'||chr(10)||chr(10);

    end loop;

    v_script := v_script || ' commit;';

    return v_script;

  end;

  function build_sql(i_name varchar2) return clob
  is
    v_db_link         varchar2(60);
    v_source          varchar2(100);
    v_target          varchar2(100);
    v_filter          varchar2(4000);
    v_source_hint     varchar2(4000);
    v_target_hint     varchar2(4000);
    v_delta_column    varchar2(50);
    v_last_delta      varchar2(1000);
    v_source_cols     long;
    v_target_cols     long;
    v_delta_data_type varchar2(50);
  begin

    select
      db_link, source, target, filter, source_hint, target_hint, v_delta_column, last_delta
    into
      v_db_link, v_source, v_target, v_filter, v_source_hint, v_target_hint, v_delta_column, v_last_delta
    from
      etl_utl.elo_tables
    where
      name = i_name;

    if trim(v_target_hint) is not null and instr(v_target_hint,'/*+') = 0
    then
      v_target_hint := '/*+ '||v_target_hint||' */';
    end if;

    if trim(v_source_hint) is not null and instr(v_source_hint,'/*+') = 0
    then
      v_source_hint := '/*+ '||v_source_hint||' */';
    end if;

    if v_delta_column is not null then
      v_delta_data_type := fun_get_delta_col_type(v_db_link, v_source, v_delta_column);
    end if;

    if v_filter is not null or v_delta_column is not null then
      v_filter := 'WHERE ' || v_filter;
      if v_delta_column is not null then
        v_filter := v_filter || ' AND '||v_delta_column||'>'||
        case v_delta_data_type
          when 'DATE'     then pl.date_string(to_date(v_last_delta,gv_date_format))
          when 'NUMBER'   then to_number(v_last_delta)
          when 'CHAR'     then v_last_delta
          when 'VARCHAR'  then v_last_delta
          else v_last_delta
        end;
      end if;
    end if;


    for c in (select source_col, target_col from etl_utl.elo_columns where name = i_name) loop
       v_source_cols := v_source_cols || c.source_col || ',';
       v_target_cols := v_target_cols || c.target_col || ',';
    end loop;

    v_source_cols := rtrim(v_source_cols, ',');
    v_target_cols := rtrim(v_target_cols, ',');

    gv_sql := '
      INSERT '||v_target_hint||' INTO '|| v_target ||'
      ('||v_target_cols|| ')
      SELECT '||v_source_hint||'
      '||v_source_cols||'
      FROM
        '||v_source||'@'||v_db_link||'
      '||v_filter||'';

    return gv_sql;

  end;

  function simulate(i_name varchar2) return clob
  is
  begin
    return build_sql(i_name);
  end;

end;
/
