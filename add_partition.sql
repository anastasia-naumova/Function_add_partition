create or replace function add_partition(sname text, tname text, to_dt text, partition_interval text, check_interval bool default True)
returns text as $$
declare 
 max_part_date timestamp;
 table_interval int;
 interval_ok bool;
 v_q text;
 res text;
begin
WITH recursive inh
AS (
	SELECT i.inhrelid, NULL::TEXT AS parent
	FROM pg_catalog.pg_inherits i
	JOIN pg_catalog.pg_class cl
		ON i.inhparent = cl.oid
	JOIN pg_catalog.pg_namespace nsp
		ON cl.relnamespace = nsp.oid
	WHERE nsp.nspname = sname AND cl.relname = tname
	UNION ALL	
	SELECT i.inhrelid, (i.inhparent::regclass)::TEXT
	FROM inh
	JOIN pg_catalog.pg_inherits i
		ON (inh.inhrelid = i.inhparent)
	), a
AS (
	SELECT c.relname AS partition_name, n.nspname AS partition_schema, pg_get_expr(c.relpartbound, c.oid, true) AS partition_expression
	FROM inh
	JOIN pg_catalog.pg_class c
		ON inh.inhrelid = c.oid
	JOIN pg_catalog.pg_namespace n
		ON c.relnamespace = n.oid
	LEFT JOIN pg_partitioned_table p
		ON p.partrelid = c.oid
	ORDER BY n.nspname, c.relname
	), b
AS (
	SELECT partition_name, regexp_matches(partition_expression, '(\d+[^'')]+)\D+(\d+[^'')]+)', 'g')::TIMESTAMP array re_d
	FROM a
	), f
AS (
	SELECT re_d[1] start_dt, re_d[2] end_dt, re_d[1] + partition_interval::interval - re_d[2] dif_intrv
	FROM b
	), max_d
AS (
	SELECT max(end_dt) max_dttm
	FROM f
	), ex_interv
AS (
	SELECT count(*) exists_interv
	FROM f
	WHERE dif_intrv = '00:00:00'
	)	
SELECT max_dttm, exists_interv into max_part_date, table_interval
FROM f;

res:=''; 

interval_ok = true;

if check_interval then
   if table_interval = 0 
     then interval_ok = false;
       raise exception 'Таблица % не партиционирована по интервалу %', sname||'.'||tname, partition_interval;
    end if;
end if;

if max_part_date is null 
   then 
	raise exception 'Таблица не существует или не партиционирована %', sname||'.'||tname;
else
  if interval_ok then
 		if max_part_date < to_dt::timestamp then 
		 while (max_part_date <= to_dt::timestamp)
		 loop
			v_q := 'create table ' || sname || '.' ||tname ||'_'||to_char(max_part_date,'yyyy_MM_dd_HH24_MI_ss') ||
     		' partition of ' || sname || '.' ||tname ||' for values from('''
     		|| max_part_date || ''') to(''' || max_part_date + (partition_interval)::interval || ''')'; 
    	 	raise notice  '%', v_q;
    	 	execute v_q;
    	 	max_part_date:= max_part_date + (partition_interval)::interval;
    	 	res:= res||v_q||';'||chr(10);
 		 end loop;
 		 return res;
         else 
 		   raise notice  '%', max_part_date;
 		   return 'Таблица партициоированна до ' || max_part_date; 	 	
		end if;
	end if;
end if;
end;
$$ language plpgsql;
