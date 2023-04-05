--drop user etl_utl cascade;

--create user etl_utl identified by etl_utl123;

grant connect, resource to etl_utl;

grant select on sys.dba_constraints to etl_utl;

grant select on sys.dba_indexes to etl_utl;

grant select on sys.dba_objects to etl_utl;

grant select on sys.v_$lock to etl_utl;

grant select on sys.v_$session to etl_utl;

grant select on sys.v_$locked_object to etl_utl;

grant select on sys.v_$lock to etl_utl;

grant select on sys.v_$session  to etl_utl;

grant execute on sys.utl_mail to etl_utl;

grant select on dba_tab_partitions to etl_utl;

grant select on dba_tab_subpartitions to etl_utl;