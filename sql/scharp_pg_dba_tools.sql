CREATE SCHEMA IF NOT EXISTS tools;

GRANT USAGE ON SCHEMA tools TO PUBLIC;

CREATE OR REPLACE FUNCTION tools.pg_stat_progress_vacuum ()
RETURNS TABLE (
  database_name name,
  schema_name name,
  table_name name,
  pid integer,
  relid oid,
  phase text,
  heap_blks_total bigint,
  heap_blks_scanned bigint,
  heap_blks_vacuumed bigint,
  index_vacuum_count bigint,
  max_dead_tuples bigint,
  num_dead_tuples bigint
) AS
$body$
SELECT 
  a.datname,
  c.nspname, 
  b.relname, 
  a.pid,
  a.relid,
  a.phase,
  a.heap_blks_total,
  a.heap_blks_scanned,
  a.heap_blks_vacuumed,
  a.index_vacuum_count,
  a.max_dead_tuples,
  a.num_dead_tuples
FROM pg_stat_progress_vacuum a 
LEFT JOIN pg_class b on a.relid = b.oid AND (a.datname = current_database() OR a.relid < 20000)
LEFT JOIN pg_namespace c ON b.relnamespace = c.oid AND (a.datname = current_database() OR a.relid < 20000);
$body$
LANGUAGE 'sql'
VOLATILE
CALLED ON NULL INPUT
SECURITY DEFINER;

GRANT EXECUTE ON FUNCTION tools.pg_stat_progress_vacuum () TO PUBLIC;

CREATE OR REPLACE FUNCTION tools.planning_time (
  queries text []
)
RETURNS TABLE (
  query text,
  planning_time numeric
) AS
$body$
/*
Collectd - Sample Implementation
  <Query planning_time>
    Statement "SELECT * FROM @extschema@.planning_time(ARRAY[ARRAY['item_data', E'SELECT * FROM study.item_data itemdata0_ where itemdata0_.item_group_data_id=''1238840'''],
ARRAY['item', E'SELECT * FROM study.item item1_ where item1_.oid=''LB1.LBAEPLAT'''],
ARRAY['item_data_item', E'select
        itemdata0_.item_data_id as item_dat1_10_0_,
    itemdata0_.first_entered_on as first_en2_10_0_,
    itemdata0_.is_active as is_activ3_10_0_,
    itemdata0_.is_deleted as is_delet4_10_0_, itemdata0_.item_id as item_id9_10_0_, itemdata0_.item_group_data_id as item_gr10_10_0_,
    itemdata0_.lab_analyte_unit as lab_anal5_10_0_, itemdata0_.last_modified as last_mod6_10_0_, itemdata0_.measurement_unit_id as measure11_10_0_,
    itemdata0_.site_id as site_id12_10_0_, itemdata0_.update_checkpoint_id as update_13_10_0_, itemdata0_.value as value7_10_0_,
    itemdata0_.value_foreign_language as value_fo8_10_0_
    from (SELECT * FROM study.item_data itemdata0_ where itemdata0_.item_group_data_id=''1238840'') itemdata0_
    inner join (SELECT * FROM study.item item1_ where item1_.oid=''LB1.LBAEPLAT'') item1_ on itemdata0_.item_id=item1_.item_id']]);"
    <Result>
      Type pg_planning_time
      InstancesFrom "query"
      ValuesFrom planning_time
    </Result>
  </Query>
*/
DECLARE
    r RECORD;
    time_scale TEXT;
    sub_query TEXT[];
BEGIN
    FOREACH sub_query SLICE 1 IN ARRAY queries LOOP
        query := sub_query[1];
        FOR r IN EXECUTE 
            'EXPLAIN (SUMMARY)' || sub_query[2]
            LOOP
    
            IF r."QUERY PLAN" LIKE 'Planning time%' THEN
                time_scale := substr(r."QUERY PLAN", length(r."QUERY PLAN")-(strpos(reverse(r."QUERY PLAN"),' ')-2));
                planning_time :=  substr(r."QUERY PLAN",16,(length(r."QUERY PLAN")-length(time_scale)-16));
                CASE time_scale
                    WHEN 's' THEN
                        planning_time := planning_time * 1000;
                    ELSE
                END CASE;
                --RAISE NOTICE 'Plan: "%" "%"', planning_time, time_scale;
            END IF;
        END LOOP;
        RETURN NEXT;
    END LOOP;
END;
$body$
LANGUAGE 'plpgsql'
VOLATILE
CALLED ON NULL INPUT
SECURITY INVOKER;

GRANT EXECUTE ON FUNCTION tools.planning_time (queries text []) TO PUBLIC;

