create or replace function add_partition(sname text, tname text, to_dt text, partition_interval text, check_interval bool default true, all_or_any text default 'all')
returns text as $$
declare
 max_part_date timestamp;
 flg bool;
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
AS ( /*Проверка равенства инрервалов партиционирования и переданного интервала*/
	SELECT re_d[2] end_dttm, re_d[1] + partition_interval::interval - re_d[2] dif_intrv
	FROM b
	), max_d
AS (
	SELECT max(end_dttm) max_dttm
	FROM f
	), flag 
AS ( /*Флаг проверки интервала*/
	SELECT CASE WHEN all_or_any = 'any' 
	THEN '00:00:00'::interval =any (SELECT dif_intrv FROM f)
	ELSE '00:00:00'::interval =all (SELECT dif_intrv FROM f) 
	END flag_intrv 
	)
SELECT max_dttm, flag_intrv INTO max_part_date, flg
FROM max_d, flag;
res:=''; 

if check_interval then
	if not flg then
		raise exception 'Таблица % партиционирована по нескольким интервалам', sname||'.'||tname;
    end if;
end if;

if max_part_date is null then 
	raise exception 'Таблица не существует или не партиционирована %', sname||'.'||tname;
elsif max_part_date >= to_dt::timestamp  then
	raise notice  '%', max_part_date;
	return 'Таблица партициоированна до ' || max_part_date;
else
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
end if;
end;
$$ language plpgsql;
