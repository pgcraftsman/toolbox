-- pgcraftsman-snapshots.sql
-- V1.8
--
-- SRM: Scott Mead, PgCraftsman
--  scott@pgcraftsman.io
--
-- FA: Farrukh Afzal, OpenSCG Inc.
-- farrukha@openscg.com
--
-- This script will setup a snapshot infrastructure inside of a
-- postgres database.  It will monitor table usage and growth
-- statistics and let you report over them
--
-- Usage:
--  1. Run this script in its entirty in a database
--     - You must have privilege to create schemas, tables, sequences
--        types and functions as well as read the pg_stat_user_table view
--
-- 2. Run a cronjob to periodically create a snapshot
--  i.e.
--  */5 * * * * psql -d db -U user -c "select snapshots.save_snap();"
--
-- 3. To view available snapshots for reporting, run:
--  select * from snapshots.list_snaps();
--
-- 4. To generate a report, run
--  select * from report_tables(from snap_id , to snap_id);
--  select * from report_indexes(from snap_id , to snap_id);
--
--  i.e.
--  select * from report_tables(1,100) will generate a report
--  of what happened between snapshot 1 and 100
--
-- To just see a report of the MAX snapshot, you can run:
--
--  select * from report_indexes(1,0);
--  Using a '0' will automatically choose the highest snapshot
--  Using a '0' in the 'from' snap field will choose the MAX snap -1
--
-- Changelog
-- Author|   Date      |  Ver. |  Comment
-- ===================================================
-- SRM   | 2014-02-05  |  1.0  |  First version
-- SRM   | 2014-02-06  |  1.1  |  * Renamed table for table stats
--                             |  * Maintaining separate snap list
--                             |  * Added saving and reporting of index stats
--                             |  * Added saving of stat_activity ( no reporting yet )
-- FA    | 2014-07-10  | 1.2   |  Added saving of database stats.
-- SRM   | 2014-08-15  | 1.3   |  Fixed index reporting function to join on table & indexname
-- SRM   | 2015-01-21  | 1.4   |  Added tables for CPU & Load Average savings for
--                             |   integration with Sigar java-based monitoring
-- SRM   | 2015-02-03  | 1.5   |  Added foreign keys with ON DELETE CASCADE for
--                             |  * 'easy' purging of snapshots ( just delete from snap.snap where... )
--                             |  * Added indexing for io tables
-- SRM   | 2015-02-20  | 1.6   |  Added pg_locks data capture
-- SRM   | 2015-03-05  | 1.7   |  Removed snap_settings from the truncate logic
--                             |  * We want to keep the settings always
--                             |  * Added memory snapshots
-- SRM   | 2015-09-10  | 1.8   |  Added pg_statio_all_tables

set application_name="OpenWatch-Install";

create schema snapshots;

-- pg_stat_user_tables
create table snapshots.snap_user_tables as
   select 1 snap_id, now() dttm, *, pg_relation_size(relid::regclass) relsize,
               pg_total_relation_size(relid::regclass) totalrelsize
     from pg_stat_user_tables where schemaname not in ('rdsadmin');

create index idx_snap_user_tables_snap_id
      on snapshots.snap_user_tables(snap_id);

-- pg_statio_all_tables
create table snapshots.snap_statio_all_tables as
   select 1 snap_id, now() dttm, *
     from pg_statio_all_tables where schemaname not in ('rdsadmin');
create index idx_snap_statio_all_tables
      on snapshots.snap_statio_all_tables(snap_id);

-- pg_stat_activity
create table snapshots.snap_stat_activity as
   select 1 snap_id, now() dttm, *
     from pg_stat_activity;
create index idx_snap_stat_activity_snap_id
      on snapshots.snap_stat_activity(snap_id);
--pg_locks
create table snapshots.snap_pg_locks as
  select 1 snap_id, now() dttm, *
    from pg_locks;
create index idx_snap_pg_locks_snap_id on snapshots.snap_pg_locks(snap_id);
create index idx_snap_pg_locks_dttm on snapshots.snap_pg_locks(dttm);
create index idx_snap_pg_locks_pid on snapshots.snap_pg_locks(pid);

-- pg_stat_user_indexes
create table snapshots.snap_indexes as
   select 1 snap_id, now() dttm, *,
        pg_relation_size(relid::regclass) relsize,
    pg_relation_size(indexrelid::regclass) indexrelsize,
        pg_total_relation_size(relid::regclass) totalrelsize
     from pg_stat_user_indexes where schemaname not in ('rdsadmin');

create index idx_snap_indexes_snap_id on snapshots.snap_indexes(snap_id);

--pg_stat_database
create table snapshots.snap_databases as
   select 1 snap_id, now() dttm, *,
        pg_database_size(datname::text) dbsize
     from pg_stat_database where datname not in ('rdsadmin');

create index idx_snap_databases_snap_id on snapshots.snap_databases(snap_id);

create table snapshots.snap_settings as
    select 1 snap_id, now() dttm, name, setting
     from pg_settings;

create index idx_snap_settings_snap_id on snapshots.snap_settings(snap_id);
create index idx_snap_settings_snap_name on snapshots.snap_settings(name);
create index idx_snap_settings_snap_name_setting on snapshots.snap_settings(name,setting);

--Snapshot parent table
create table snapshots.snap as
    SELECT 1 snap_id, now() dttm;

create index idx_snap_snap_id on snapshots.snap(snap_id);
create index idx_snap_snap_dttm on snapshots.snap(dttm);

-- CPU data
create table snapshots.snap_cpu ( snap_id     int,
                                  cpu_id      int,
                                  dttm        TIMESTAMP default now(),
                                  usertime    decimal,
                                  systime     decimal,
                                  idletime    decimal,
                                  waittime    decimal,
                                  nicetime    decimal,
                                  combined    decimal,
                                  irqtime     decimal,
                                  softirqtime decimal,
                                  stolentime  decimal );

create index idx_snap_cpu_snap_id on snapshots.snap_cpu(snap_id);
create index idx_snap_cpu_cpu_id on snapshots.snap_cpu(cpu_id);
create index idx_snap_cpu_usertime on snapshots.snap_cpu(usertime);

-- load average storage
create table snapshots.snap_load_avg ( snap_id  int,
                                       dttm     TIMESTAMP default now(),
                                       load5    decimal,
                                       load10   decimal,
                                       load15   decimal );

create index idx_snap_load_avg_snap_id on snapshots.snap_load_avg(snap_id);

-- iostat storage
create table snapshots.snap_iostat ( snap_id    int,
                                     dttm       TIMESTAMP default now(),
                                     filesystem TEXT,
                                     mountpoint TEXT,
                                     reads      NUMERIC,
                                     writes     NUMERIC,
                                     rbytes     NUMERIC,
                                     wbytes     NUMERIC,
                                     queue      NUMERIC,
                                     svctm      NUMERIC );

create index idx_snap_iostat_snap_id on snapshots.snap_iostat(snap_id);
create index idx_snap_iostat_filesystem on snapshots.snap_iostat(filesystem);
create index idx_snap_iostat_mountpoint on snapshots.snap_iostat(mountpoint);

-- memory storage
create table snapshots.snap_mem ( snap_id int,
                  dttm    TIMESTAMP default now(),
                  metric  TEXT,
                  total   NUMERIC,
                  used    NUMERIC,
                  free    NUMERIC);

create index idx_snap_mem_snap_id on snapshots.snap_mem(snap_id);
create index idx_snap_mem_metric on snapshots.snap_mem(metric);

-- Add pkeys and fkeys to the tables
alter table snapshots.snap
  add constraint snap_pk PRIMARY KEY ( snap_id );

alter table snapshots.snap_user_tables
  add constraint sut_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_stat_activity
  add constraint sst_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_indexes
  add constraint si_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_databases
  add constraint sd_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_pg_locks
  add constraint spl_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_cpu
  add constraint scp_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_load_avg
  add constraint sla_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

alter table snapshots.snap_iostat
  add constraint sio_snap_id foreign key (snap_id )
  references snapshots.snap ( snap_id ) on delete cascade;

-- Start at two because we manually create a snapshot
-- of 1 above when we create the snapshot tables.
create sequence snapshots.snap_seq start 2;

--Insert into snap
-- This is our primary snapshot function.  It's designed to save
--  all of the data from the system catalogs in to our snapshot
--  for reporting across later.
--
create or replace function snapshots.save_snap () RETURNS INT as $_$
DECLARE
     i_snap_id INT;
BEGIN
    i_snap_id := nextval('snapshots.snap_seq');

    insert into snapshots.snap select i_snap_id, now() ;

    insert into snapshots.snap_user_tables select i_snap_id, now(), *, pg_relation_size(relid::regclass) relsize,
               pg_total_relation_size(relid::regclass) totalrelsize from pg_stat_user_tables where schemaname not in ('rdsadmin');

    insert into snapshots.snap_statio_all_tables select i_snap_id, now(), *
           from pg_statio_all_tables where schemaname not in ('rdsadmin');

    insert into snapshots.snap_stat_activity SELECT i_snap_id, now(), * from pg_stat_activity;

    insert into snapshots.snap_indexes SELECT i_snap_id, now(), * ,
                pg_relation_size(relid::regclass) relsize,
                pg_relation_size(indexrelid::regclass) indexrelsize,
                pg_total_relation_size(relid::regclass) totalrelsize
            from pg_stat_user_indexes WHERE schemaname not in ('rdsadmin');

    insert into snapshots.snap_databases SELECT i_snap_id, now(), * ,
                pg_database_size(datname::text) dbsize
            from pg_stat_database
            WHERE datname NOT IN ('template0','template1','rdsadmin')
            ;

    insert into snapshots.snap_pg_locks SELECT i_snap_id, now(), *
           from pg_locks;

    insert into snapshots.snap_settings SELECT i_snap_id, now(), ps.name, ps.setting
            from pg_settings ps, snapshots.snap_settings sns
             WHERE sns.snap_id =
                 ( SELECT max(snap_id) from snapshots.snap_settings )
               AND ps.name = sns.name
               AND ps.setting <> sns.setting;

    RETURN i_snap_id;
END; $_$ language 'plpgsql';


select snapshots.save_snap();

create or replace function snapshots.delete_snaps(snap_from INT, snap_to INT )
  RETURNS VOID AS $_$
DECLARE
  start_id INT;
  end_id   INT;
BEGIN
  IF snap_to = 0
     THEN
          select into end_id max(snap_id) from snapshots.snap ;
     ELSE
          end_id := snap_to;
     END IF;

     IF snap_from = 0
     THEN
          start_id = end_id - 1;
     ELSE
          start_id = snap_from;
     END IF;

     DELETE FROM snapshots.snap where snap_id between start_id AND end_id;

END $_$ language 'plpgsql';

create type snapshots.snap_list AS ( snaps_id int, dttm timestamp with time zone ) ;

create or replace function snapshots.list_snaps() RETURNS SETOF snapshots.snap_list AS $_$
     select distinct snap_id, dttm from snapshots.snap order by snap_id;
 $_$ language 'sql';


create type snapshots.report_tables_record AS (
   time_window interval, relname name, ins bigint, upd bigint, del bigint,
     index_scan bigint, seqscan bigint, relsize_growth_bytes bigint, relsize_growth text,
     total_relsize_growth_bytes bigint, total_relsize_growth text, total_relsize bigint,
     total_relsize_bytes text);

create or replace function snapshots.report_tables
      ( snap_from INT, snap_to INT )
      RETURNS SETOF snapshots.report_tables_record AS $_$
DECLARE
     start_id INT;
     end_id  INT;
     start_date TIMESTAMP WITH TIME ZONE;
     end_date TIMESTAMP WITH TIME ZONE;
     query TEXT;

BEGIN

     IF snap_to = 0
     THEN
          select into end_id max(snap_id) from snapshots.snap_user_tables ;
     ELSE
          end_id := snap_to;
     END IF;

     select into end_date dttm from snapshots.snap where snap_id = end_id limit 1;

     IF snap_from = 0
     THEN
          start_id = end_id - 1;
     ELSE
          start_id = snap_from;
     END IF;

    select into start_date dttm from snapshots.snap where snap_id = start_id limit 1;

     RAISE NOTICE 'Report   From  Snapshot # % Taken at %' , start_id, start_date ;
     RAISE NOTICE 'Report   To       Snapshot # % Taken at %' , end_id, end_date ;

     query := 'select b.dttm - a.dttm ,  b.relname, b.n_tup_ins - a.n_tup_ins ins, b.n_tup_upd - a.n_tup_upd upd, b.n_tup_del - a.n_tup_del del, '
             || 'b.idx_scan - a.idx_scan index_scan, b.seq_scan - a.seq_scan seqscan, b.relsize - a.relsize relsize_growth_bytes, '
             || 'pg_size_pretty( b.relsize - a.relsize) relsize_growth, b.totalrelsize - a.totalrelsize total_relsize_growth_bytes, '
             || 'pg_size_pretty(b.totalrelsize - a.totalrelsize) total_relsize_growth, b.totalrelsize total_relsize, '
             || 'pg_size_pretty(b.totalrelsize) total_relsize_bytes '
             || 'from snapshots.snap_user_tables a , snapshots.snap_user_tables b '
             || 'where a.snap_id=$1 '
             || 'and b.snap_id=$2 '
             || 'and a.relid=b.relid '
             || 'order by ( (b.n_tup_ins - a.n_tup_ins ) + (b.n_tup_upd - a.n_tup_upd ) + (b.n_tup_del - a.n_tup_del)) desc ';

     RETURN QUERY EXECUTE query USING start_id, end_id;

END; $_$ language 'plpgsql';


create type snapshots.report_indexes_record AS ( time_window interval, relname name, indexrelname name, idx_scan bigint,
                                                 idx_tup_read bigint, idx_tup_fetch bigint, relsize_growth_bytes bigint,relsize_growth text,
                                                 total_relsize_growth_bytes bigint, total_relsize_growth text );

create or replace function snapshots.report_indexes ( snap_from INT, snap_to INT ) RETURNS SETOF snapshots.report_indexes_record AS $_$
DECLARE
     start_id INT;
     end_id  INT;
     start_date TIMESTAMP WITH TIME ZONE;
     end_date TIMESTAMP WITH TIME ZONE;
     query TEXT;

BEGIN

     IF snap_to = 0
     THEN
          select into end_id max(snap_id) from snapshots.snap ;
     ELSE
          end_id := snap_to;
     END IF;

     select into end_date dttm from snapshots.snap where snap_id = end_id limit 1;

     IF snap_from = 0
     THEN
          start_id = end_id - 1;
     ELSE
          start_id = snap_from;
     END IF;

    select into start_date dttm from snapshots.snap where snap_id = start_id limit 1;

     RAISE NOTICE 'Report   From  Snapshot # % Taken at %' , start_id, start_date ;
     RAISE NOTICE 'Report   To       Snapshot # % Taken at %' , end_id, end_date ;

     query := 'select b.dttm - a.dttm ,  b.relname, b.indexrelname, b.idx_scan - a.idx_scan idx_scan, '
             || 'b.idx_tup_read - a.idx_tup_read idx_tup_read, b.idx_tup_fetch - a.idx_tup_fetch idx_tup_fetch, b.relsize - a.relsize relsize_growth_bytes, '
             || 'pg_size_pretty( b.relsize - a.relsize) relsize_growth, b.totalrelsize - a.totalrelsize total_relsize_growth_bytes, '
             || 'pg_size_pretty(b.totalrelsize - a.totalrelsize) total_relsize_growth '
             || 'from snapshots.snap_indexes a , snapshots.snap_indexes b '
             || 'where a.snap_id=$1 '
             || 'and b.snap_id=$2 '
             || 'and a.relid=b.relid '
             || 'and a.indexrelname = b.indexrelname '
             || 'order by (b.idx_scan - a.idx_scan )  desc ';

     RETURN QUERY EXECUTE query USING start_id, end_id;

END; $_$ language 'plpgsql';

/* Blank line @ end */

