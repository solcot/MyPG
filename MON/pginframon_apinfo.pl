#!/usr/bin/perl
#use strict; 
#use perl /work2/pg/pgdata1/TMP/IFR/MONITOR/pginframon_apinfo.pl -d $PGDB -s 0 -q 10000000 -w 1000000 -e 10000000 -t 0 -a 0 -p 0 -o /work2/pg/pgdata1/TMP/IFR/MONITOR/LOG_PERL_D -z -x -c -v -O

use Getopt::Std; 
 
my %options=(); 
getopts("hd:s:q:w:e:t:a:p:o:zxcvWRO", \%options); 
 
if($options{h}) { do_help(); } 
if($options{d}) { $db = $options{d}; } else { print "require -d option... for help -h option...\n"; exit; } 
if($options{s} or $options{s} == 0) { $scnt = $options{s}; } else { print "require -s option... for help -h option...\n"; exit; } 
if($options{q}) { $sd1 = $options{q}; } else { print "require -q option... for help -h option...\n"; exit; } 
if($options{w}) { $sd2 = $options{w}; } else { print "require -w option... for help -h option...\n"; exit; } 
if($options{e}) { $sd3 = $options{e}; } else { print "require -e option... for help -h option...\n"; exit; } 
if($options{t} or $options{t} == 0) { $tabtopcnt = $options{t}; } else { print "require -t option... for help -h option...\n"; exit; } 
if($options{a} or $options{a} == 0) { $apptopcnt = $options{a}; } else { print "require -a option... for help -h option...\n"; exit; } 
if($options{p} or $options{p} == 0) { $topappapinfo = $options{p}; } else { print "require -p option... for help -h option...\n"; exit; } 
if($options{o}) { $logfile_dir = $options{o}; } else { print "require -o option... for help -h option...\n"; exit; } 

if($options{z}) { $log = $options{z}; } 
if($options{x}) { $loglockapinfo = $options{x}; } 
if($options{c}) { $logappapinfo = $options{c}; } 
if($options{v}) { $before = $options{v}; } 

if($options{W}) { $sortwrite = $options{W}; } 
if($options{R}) { $sortread = $options{R}; } 
if($options{O}) { $adaylogging = $options{O}; } 

$sdiff = 0;     # sleep compensation

#--------------------------------------------------------------
sub timediff {
use Time::Local;
($year,$month,$day,$hour,$min,$sec) = (substr($_[0],0,4),substr($_[0],5,2),substr($_[0],8,2),substr($_[0],11,2),,substr($_[0],14,2),substr($_[0],17,2));
($year2,$month2,$day2,$hour2,$min2,$sec2) = (substr($_[1],0,4),substr($_[1],5,2),substr($_[1],8,2),substr($_[1],11,2),,substr($_[1],14,2),substr($_[1],17,2));
$btimesec = timelocal($sec,$min,$hour,$day,$month-1,$year);
$atimesec = timelocal($sec2,$min2,$hour2,$day2,$month2-1,$year2);
$timediff = $atimesec - $btimesec;
return $timediff;
}
#--------------------------------------------------------

#`db2 connect to $db`;

chomp( $logsdate = `date +"%Y%m%d"` );
$flogfile_dir = $logfile_dir . "/" . substr($logsdate, 0, 6);
`mkdir $flogfile_dir` if !(-d "$flogfile_dir");
$logfile = $flogfile_dir . "/db2inframon_$logsdate.log";
$logfile_apinfo = $flogfile_dir . "/db2apinfo_$logsdate.log";

if($adaylogging) {
if (!(-e "$logfile")) {
$out = qx{ psql -c "select to_char(current_timestamp,'YYYY-MM-DD-HH24.MI.SS') snap_date, * from pg_stat_database" };
open(OUT, ">" . $logfile . "_db"); print OUT "$out"; close(OUT);
#$out = `db2 "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50) tabname, sum(rows_read) rr, sum(rows_inserted+rows_updated+rows_deleted) rm, sum(rows_inserted) ri, sum(rows_updated) ru, sum(rows_deleted) rd, sum(table_scans) ts, max(section_exec_with_col_references) sewcr, sum(COALESCE(DATA_OBJECT_L_PAGES,0)) datpag, sum(COALESCE(LOB_OBJECT_L_PAGES,0)) lobpg, sum(COALESCE(INDEX_OBJECT_L_PAGES,0)) idxpg, sum(LOCK_ESCALS) lock_escals,sum(LOCK_WAITS) lock_waits,count(*) pcnt,current_timestamp ts from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
#open(OUT, ">" . $logfile . "_tab"); print OUT "$out"; close(OUT);
#$out = `db2 +w "select executable_id, num_exec_with_metrics, stmt_exec_time, rows_read, rows_modified, rows_returned, total_cpu_time, total_sorts, SORT_OVERFLOWS,LOCK_ESCALS,LOCK_WAITS,DEADLOCKS,LOCK_TIMEOUTS,PACKAGE_SCHEMA,PACKAGE_NAME,EFFECTIVE_ISOLATION,varchar(stmt_text,250) stmt_text,current_timestamp ts from table(mon_get_pkg_cache_stmt(null,null,'<modified_within>1440</modified_within>',-2))"`;
#open(OUT, ">" . $logfile . "_pcache"); print OUT "$out"; close(OUT);
$out = qx{ psql -c "select to_char(current_timestamp,'YYYY-MM-DD-HH24.MI.SS') snap_date, * from pg_stat_archiver" };
open(OUT, ">" . $logfile . "_archiver"); print OUT "$out"; close(OUT);
}
}

if($log) {
   open STDOUT, ">> $logfile" or die "error $!";
   open STDERR, ">> $logfile" or die "error $!";
}
if($logappapinfo or $loglockapinfo) { open LOG_APINFO, ">> $logfile_apinfo" or die "error $!"; }

#----------------------------------------------------------

################ delta1
#chomp( $bdate = `date +%Y%m%d%H%M%S` );
open(IN, $logfile_dir . "/tmpdate"); $bdate = <IN>; close(IN);
## snapdb
#$bsnapdbts = qx{psql -F ' ' -A -t -c "select current_timestamp"};
open(IN, $logfile_dir . "/tmpdbts"); $bsnapdbts = <IN>; close(IN);
if($before) {
#   $tmpdb = `db2 -x "select rows_read,rows_deleted+rows_inserted+rows_updated,commit_sql_stmts+int_commits+rollback_sql_stmts+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,0,0 from sysibmadm.snapdb with ur"`;
   open(IN, $logfile_dir . "/tmpdb"); $tmpdb = <IN>; close(IN);
} else {
#   $tmpdb = `db2 -x "select rows_read,rows_modified,total_app_commits+int_commits+total_app_rollbacks+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS from table(MON_GET_DATABASE(-2)) with ur"`;
   open(IN, $logfile_dir . "/tmpdb"); $tmpdb = <IN>; close(IN);
}
$tmpdb =~ s/^\s+|\s+$//g;
($bdb_trans, $bdb_commits, $bdb_rollbacks, $bdb_blksall, $bdb_blkshits, $bdb_blksreads, $bdb_tupreturned, $bdb_tupfetched, $bdb_tupmods, $bdb_tempfiles, $bdb_tempbytes, $bdb_deadlocks) = split /\s+/, $tmpdb;#print "$bdbcnt1, $bdbcnt2, $bdbcnt3\n";
## snaptab
if($tabtopcnt > 0) {
#$bsnaptabts = qx{psql -F ' ' -A -t -c "select current_timestamp"};
open(IN, $logfile_dir . "/tmptabts"); $bsnaptabts = <IN>; close(IN);
if($before) {
#   $btabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), trim(char(sum(rows_read))) || ':' || trim(char(sum(rows_written))) || ':' || char(0) || ':' || char(0) from sysibmadm.snaptab group by tabschema, tabname with ur"`;   
   open(IN, $logfile_dir . "/tmptab"); $btabcnt1 = do{local $/; <IN>}; close(IN);
} else {
#   $btabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), varchar(sum(rows_read)) || ':' || varchar(sum(rows_inserted+rows_updated+rows_deleted)) || ':' || varchar(sum(table_scans)) || ':' || varchar(max(section_exec_with_col_references)) from table(MON_GET_TABLE(null,null,-2)) group by tabschema, tabname with ur"`;
   open(IN, $logfile_dir . "/tmptab"); $btabcnt1 = do{local $/; <IN>}; close(IN);
}
%btabcnt1s = split /\s+/, $btabcnt1;
}
## snapappl
if($apptopcnt > 0) {
if($scnt > 0)
{ $bsnapapplts = qx{psql -F ' ' -A -t -c "select current_timestamp"}; } else
{ open(IN, $logfile_dir . "/tmpapplts"); $bsnapapplts = <IN>; close(IN); }
if($before) {
   if($scnt > 0)
   { $bapplcnt1 = `db2 -x "select varchar(replace(b.appl_name,' ','')||'.'||replace(char(b.AGENT_ID),' ',''),50), trim(char(a.rows_read)) || ':' || trim(char(a.rows_written)) from sysibmadm.snapappl a join sysibmadm.applications b on a.agent_id = b.agent_id with ur"`; } else
   { open(IN, $logfile_dir . "/tmpappl"); $bapplcnt1 = do{local $/; <IN>}; close(IN); }
} else {
   if($scnt > 0)
   { $bapplcnt1 = `db2 -x "select varchar(replace(a.application_name,' ','')||'.'||replace(char(a.application_handle),' ',''),50), varchar(rows_read) || ':' || varchar(rows_modified) from table(MON_GET_CONNECTION(null,-2)) a with ur"`; } else
   { open(IN, $logfile_dir . "/tmpappl"); $bapplcnt1 = do{local $/; <IN>}; close(IN); }
}
%bapplcnt1s = split /\s+/, $bapplcnt1;
}

################ sleep
sleep $scnt-$sdiff;

################ delta2
chomp( $adate = `date +%Y%m%d%H%M%S` );
open(OUT, ">" . $logfile_dir . "/tmpdate"); print OUT "$adate"; close(OUT);
## snapdb
$asnapdbts = qx{psql -F ' ' -A -t -c "select current_timestamp"};
open(OUT, ">" . $logfile_dir . "/tmpdbts"); print OUT "$asnapdbts"; close(OUT);
if($before) {
   #dblevel $tmpdb = qx{psql -F ' ' -A -t -c "select xact_commit+xact_rollback trnas, xact_commit, xact_rollback, blks_hit+blks_read blks_all, blks_hit, blks_read, tup_returned, tup_fetched, tup_inserted+tup_updated+tup_deleted tup_mod, temp_files, temp_bytes, deadlocks from pg_stat_database where datid = $db"};
   $tmpdb = qx{psql -F ' ' -A -t -c "select sum(xact_commit+xact_rollback) trnas, sum(xact_commit), sum(xact_rollback), sum(blks_hit+blks_read) blks_all, sum(blks_hit), sum(blks_read), sum(tup_returned), sum(tup_fetched), sum(tup_inserted+tup_updated+tup_deleted) tup_mod, sum(temp_files), sum(temp_bytes), sum(deadlocks) from pg_stat_database"};
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
} else {
   $tmpdb = `db2 -x "select rows_read,rows_modified,total_app_commits+int_commits+total_app_rollbacks+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS, total_app_commits,int_commits,total_app_rollbacks,int_rollbacks,LOCK_ESCALS,TOTAL_HASH_JOINS,HASH_JOIN_OVERFLOWS from table(MON_GET_DATABASE(-2)) with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
}
$tmpdb =~ s/^\s+|\s+$//g;
($adb_trans, $adb_commits, $adb_rollbacks, $adb_blksall, $adb_blkshits, $adb_blksreads, $adb_tupreturned, $adb_tupfetched, $adb_tupmods, $adb_tempfiles, $adb_tempbytes, $adb_deadlocks) = split /\s+/, $tmpdb;
## snaptab
if($tabtopcnt > 0) {
$asnaptabts = qx{psql -F ' ' -A -t -c "select current_timestamp"};
open(OUT, ">" . $logfile_dir . "/tmptabts"); print OUT "$asnaptabts"; close(OUT);
if($before) {
   #$atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), trim(char(sum(rows_read))) || ':' || trim(char(sum(rows_written))) || ':' || trim(char(0)) || ':' || trim(char(0)) || ':' || trim(char(sum(COALESCE(DATA_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(LOB_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(INDEX_OBJECT_PAGES,0)))) || ':' || trim(char(count(*))) || ':' || trim(char(0)) || ':' || trim(char(0)) from sysibmadm.snaptab where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;  # <V9.7 
   $atabcnt1 = qx{psql -F ' ' -A -t -c "select schemaname||'.'||relname relname, coalesce(seq_scan,0)+coalesce(idx_scan,0) ||':'|| coalesce(seq_tup_read,0)+coalesce(idx_tup_fetch,0) ||':'|| coalesce(seq_scan,0) ||':'|| coalesce(seq_tup_read,0) ||':'|| coalesce(idx_scan,0) ||':'|| coalesce(idx_tup_fetch,0) ||':'|| coalesce(n_tup_ins+n_tup_upd+n_tup_del,0) ||':'|| coalesce(n_live_tup,0) ||':'|| coalesce(n_dead_tup,0) rel_others from pg_stat_user_tables"};  # =V9.7
   open(OUT, ">" . $logfile_dir . "/tmptab"); print OUT "$atabcnt1"; close(OUT);
} else {
   $atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','') || '.' || replace(tabname,' ',''),50), varchar(sum(rows_read)) || ':' || varchar(sum(rows_inserted+rows_updated+rows_deleted)) || ':' || varchar(sum(table_scans)) || ':' || varchar(max(section_exec_with_col_references)) || ':' || varchar(sum(COALESCE(DATA_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(LOB_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(INDEX_OBJECT_L_PAGES,0))) || ':' || varchar(count(*)) || ':' || varchar(sum(LOCK_ESCALS)) || ':' || varchar(sum(LOCK_WAITS)) from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmptab"); print OUT "$atabcnt1"; close(OUT);
}
%atabcnt1s = split /\s+/, $atabcnt1;
}
## snapappl
if($apptopcnt > 0) {
$asnapapplts = qx{psql -F ' ' -A -t -c "select current_timestamp"};
if($scnt == 0) { open(OUT, ">" . $logfile_dir . "/tmpapplts"); print OUT "$asnapapplts"; close(OUT); }
if($before) {
   $aapplcnt1 = `db2 -x "select varchar(replace(b.appl_name,' ','')||'.'||replace(char(b.AGENT_ID),' ',''),50), trim(char(a.rows_read)) || ':' || trim(char(a.rows_written)) from sysibmadm.snapappl a join sysibmadm.applications b on a.agent_id = b.agent_id with ur"`;
   if($scnt == 0) { open(OUT, ">" . $logfile_dir . "/tmpappl"); print OUT "$aapplcnt1"; close(OUT); }
} else {
   $aapplcnt1 = `db2 -x "select varchar(replace(a.application_name,' ','')||'.'||replace(char(a.application_handle),' ',''),50), varchar(rows_read) || ':' || varchar(rows_modified) from table(MON_GET_CONNECTION(null,-2)) a with ur"`;
   if($scnt == 0) { open(OUT, ">" . $logfile_dir . "/tmpappl"); print OUT "$aapplcnt1"; close(OUT); }
}
%aapplcnt1s = split /\s+/, $aapplcnt1;
}

################ delta time calc
$snapdbtimediff = timediff $bsnapdbts, $asnapdbts;
if($tabtopcnt > 0) { $snaptabtimediff = timediff $bsnaptabts, $asnaptabts; }
if($apptopcnt > 0) { $snapappltimediff = timediff $bsnapapplts, $asnapapplts; }

################ delta calc
## snapdb
$ddb_trans = sprintf("%.2f", ($adb_trans-$bdb_trans)/$snapdbtimediff);
$gddbcnt3 = "#" x ($ddbcnt3/$sd2);
$ddb_commits = sprintf("%.2f", ($adb_commits-$bdb_commits)/$snapdbtimediff);
$ddb_rollbacks = sprintf("%.2f", ($adb_rollbacks-$bdb_rollbacks)/$snapdbtimediff);
$ddb_blksall = sprintf("%.2f", ($adb_blksall-$bdb_blksall)/$snapdbtimediff);
$ddb_blkshits = sprintf("%.2f", ($adb_blkshits-$bdb_blkshits)/$snapdbtimediff);
$ddb_blksreads = sprintf("%.2f", ($adb_blksreads-$bdb_blksreads)/$snapdbtimediff);
$ddb_tupreturned = sprintf("%.2f", ($adb_tupreturned-$bdb_tupreturned)/$snapdbtimediff);
$gddbcnt1 = "#" x ($ddbcnt1/$sd1);
$ddb_tupfetched = sprintf("%.2f", ($adb_tupfetched-$bdb_tupfetched)/$snapdbtimediff);
$ddb_tupmods = sprintf("%.2f", ($adb_tupmods-$bdb_tupmods)/$snapdbtimediff);
$gddbcnt2 = "#" x ($ddbcnt2/$sd2);
$ddb_tempfiles = sprintf("%.2f", ($adb_tempfiles-$bdb_tempfiles)/$snapdbtimediff);
$ddb_tempbytes = sprintf("%.2f", ($adb_tempbytes-$bdb_tempbytes)/$snapdbtimediff);
$ddb_deadlocks = sprintf("%.2f", ($adb_deadlocks-$bdb_deadlocks)/$snapdbtimediff);
#$ddbcnt1 = int(($adbcnt1-$bdbcnt1)/$snapdbtimediff);
## snaptab
if($tabtopcnt > 0) {
%dtabcnt1 = ();
foreach $k1 (keys %atabcnt1s) {
        foreach $k2 (keys %btabcnt1s) {
                                if($k1 eq $k2) {
                                @atabs = split /:/, $atabcnt1s{$k1};
                                @btabs = split /:/, $btabcnt1s{$k2};
                                $dtabcnt1{$k1} = sprintf("%.2f",($atabs[0] - $btabs[0])/$snaptabtimediff) . ":" . 
												 sprintf("%.2f",($atabs[1] - $btabs[1])/$snaptabtimediff) . ":" .
												 sprintf("%.2f",($atabs[2] - $btabs[2])/$snaptabtimediff) . ":" .
												 sprintf("%.2f",($atabs[3] - $btabs[3])/$snaptabtimediff) . ":" .
												 sprintf("%.2f",($atabs[4] - $btabs[4])/$snaptabtimediff) . ":" .
												 sprintf("%.2f",($atabs[5] - $btabs[5])/$snaptabtimediff) . ":" .
												 sprintf("%.2f",($atabs[6] - $btabs[6])/$snaptabtimediff) . ":" .
												 int($atabs[7]) . ":" .
												 int($atabs[8]);
                                next;
                                }                               
        }
}
}
## snapappl
if($apptopcnt > 0) {
%dapplcnt1 = ();
foreach $k1 (keys %aapplcnt1s) {
        foreach $k2 (keys %bapplcnt1s) {
                                if($k1 eq $k2) {
                                @aappls = split /:/, $aapplcnt1s{$k1};
                                @bappls = split /:/, $bapplcnt1s{$k2};
                                $dapplcnt1{$k1} = int(($aappls[0] - $bappls[0])/$snapappltimediff) . ":" . int(($aappls[1] - $bappls[1])/$snapappltimediff);
                                next;
                                }       
        }
}
}

################ current calc
## cpu
# cpu
#$vmstat = `vmstat -WI 1 2 |tail -1`;  # > aix 5.3
$vmstat = `vmstat 1 2 |tail -1`;  # >= linux 7
$vmstat =~ s/^\s+|\s+$//g;
($rq,$bq,$swap,$free,$buff,$cache,$pi,$po,$fi,$fo,$in,$cs,$us,$sy,$id,$wa) = (split /\s+/, $vmstat)[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15];  # >= linux 7
#($rq,$bq,$pq,$wq,$avm,$fre,$fi,$fo,$pi,$po,$in,$sc,$cs,$us,$sy,$id,$wa) = (split /\s+/, $vmstat)[0,1,2,3,4,5,6,7,8,9,12,13,14,15,16,17,18];  # > aix 5.3
#($rq,$bq,$pi,$po,$us,$sy,$id,$wa) = (split /\s+/, $vmstat)[0,1,7,8,-3,-2,-1,2];   # hp-ux
$cpu = 100 - $id;
$gcpu = "#" x ($cpu/2) . "-" x (50-$cpu/2);
## lock
if($before) {
   #$lock = qx{psql -F ' ' -A -t -c "select (pg_blocking_pids(pid))[1], (pg_blocking_pids(pid))[2], (pg_blocking_pids(pid))[3], (pg_blocking_pids(pid))[4] from pg_stat_activity where CARDINALITY(pg_blocking_pids(pid)) > 0"};
   $lock = qx{psql -F ' ' -A -t -c "select pid
                        from pg_stat_activity
                        where pid <> pg_backend_pid()
                        and cardinality(pg_blocking_pids(pid)) = 0
                        and pid in (select unnest(pg_blocking_pids(pid)) from pg_stat_activity where cardinality(pg_blocking_pids(pid)) > 0)
                        "};
} else {
   $lock = `db2 -x "select a.hld_application_handle, a.tabname from sysibmadm.mon_lockwaits a
        where a.hld_application_handle not in (select req_application_handle from sysibmadm.mon_lockwaits) group by a.hld_application_handle, a.tabname with ur"`;
}
@locks = split /\s+/, $lock;
## connection
if($before) {
#dblevel   $conn = qx{psql -F ' ' -A -t -c "select 
#dblevel						(select numbackends from pg_stat_database where datid = $db) cur_cons,
#dblevel						(select count(*) from pg_stat_activity where state = 'active' and pid <> pg_backend_pid() and datid = $db) act_cons,
#dblevel						(select count(*) from pg_stat_activity where CARDINALITY(pg_blocking_pids(pid)) > 0 and datid = $db) lock_wait_cons"};
   $conn = qx{psql -F ' ' -A -t -c "select 
						(select sum(numbackends) from pg_stat_database) cur_cons,
						(select count(*) from pg_stat_activity where state = 'active' and pid <> pg_backend_pid()) act_cons,
						(select count(*) from pg_stat_activity where CARDINALITY(pg_blocking_pids(pid)) > 0) lock_wait_cons"};
} else {
   $conn = `db2 -x "select APPLS_CUR_CONS, APPLS_IN_DB2, NUM_LOCKS_WAITING,ACTIVE_SORTS,ACTIVE_HASH_JOINS from table(MON_GET_DATABASE(-2)) with ur"`;
}
@conns = split /\s+/, $conn;

################ print
## timediff
print "dbsecdiff: $snapdbtimediff tabsecdiff: $snaptabtimediff applsecdiff: $snapappltimediff\n";
## cpu
#aix#$avmm = int($avm*4/1024);
#aix#$frem = int($fre*4/1024);
#aix#print "$adate CPU $cpu < R: $rq B: $bq P: $pq W: $wq AVM: $avmm FRE: $frem FI: $fi FO: $fo PI: $pi PO: $po IN: $in SC: $sc CS: $cs U: $us S: $sy W: $wa I: $id >\n";
print "$adate CPU $cpu < R: $rq B: $bq PG: $swap FRE: $free BUFF: $buff CACHE: $cache FI: $fi FO: $fo PI: $pi PO: $po IN: $in CS: $cs U: $us S: $sy W: $wa I: $id >\n";
print "$adate CPU> $gcpu\n";
## lock
print "$adate Lock_holder : @locks\n";
if($loglockapinfo){
while(defined($flockap = shift @locks)) {
        if(($flockap+0) eq $flockap) {
		   $flockapinfo = qx{psql -F ' ' -A -t -c "select * from pg_stat_activity where pid = $flockap"};
           print LOG_APINFO "$adate flockapinfo $flockapinfo";
        }
}
}
## snapdb
print "$adate Lock $ddb_deadlocks : $conns[2]\n"; #deadlock, lockwaiting
#print "$adate Sort $ddbcnt16 $ddbcnt17 : $conns[4]\n"; #total,overflow,active
#print "$adate HSjoin $ddbcnt23 $ddbcnt24 : $conns[5]\n"; #total,overflow,active
#print "$adate CPU_time $ddbcnt11 : $ddbcnt12\n"; #time,latchwait
print "$adate Connection : $conns[0] $conns[1]\n"; #total,active
print "$adate Trans $ddb_trans ( $ddb_commits $ddb_rollbacks ) > $gddbcnt3\n"; #tran,commit,rollback
#print "$adate Throughput $ddbthrouput : $ddbcnt5 $ddbcnt6 $ddbcnt7 : $ddbcnt8 $ddbcnt9 $ddbcnt10\n"; #8+9+10,static,dynamic,failed,select,iud,ddl
print "$adate Rows_read $ddb_tupreturned ( $ddb_tupfetched ) $ddb_blksall ( $ddb_blkshits $ddb_blksreads ) > $gddbcnt1\n"; # totread,idxread,totblk,hitblk,diskblk
print "$adate Rows_write $ddb_tupmods > $gddbcnt2\n";
print "$adate Temp $ddb_tempfiles $ddb_tempbytes\n";
print "-" x 20 . "\n";
## snaptab : numscans,all_reads,numtabscans,seqtupreads,numidxscans,idxtupfetches,tupmods,livetups,deadtups
if($tabtopcnt > 0) {
if($sortwrite) {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[6]  <=> (split /:/, $dtabcnt1{$a})[6]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[6])/$sd3);
        print "$adate Tab_Rows_rw $kk : $dtabcnt1{$kk} : $gdtabcnt1\n";
        $loopcnt++;
        last if($loopcnt == $tabtopcnt);
        }
}
elsif($sortread) {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[1] <=> (split /:/, $dtabcnt1{$a})[1]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[1])/$sd3);
        print "$adate Tab_Rows_rw $kk : $dtabcnt1{$kk} : $gdtabcnt1\n";
        $loopcnt++;
        last if($loopcnt == $tabtopcnt);
        }
}
else {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[1]+(split /:/, $dtabcnt1{$b})[6]  <=> (split /:/, $dtabcnt1{$a})[1]+(split /:/, $dtabcnt1{$a})[6]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[1]+(split /:/, $dtabcnt1{$kk})[6])/$sd3);
        print "$adate Tab_Rows_rw $kk : $dtabcnt1{$kk} : $gdtabcnt1\n";
        $loopcnt++;
        last if($loopcnt == $tabtopcnt);
        }
}
$loopcnt = 0;
print "-" x 20 . "\n";
}
## snapappl : read,write
if($apptopcnt > 0) {
if($sortwrite) {
        foreach $kk (sort{(split /:/, $dapplcnt1{$b})[1]  <=> (split /:/, $dapplcnt1{$a})[1]} keys %dapplcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dapplcnt1{$kk})[1])/$sd3);
        print "$adate appl_Rows_rw $kk : $dapplcnt1{$kk} : $gdapplcnt1\n";
        $loopcnt++;
        last if($loopcnt == $apptopcnt);
        if($logappapinfo) {
        if($loopcnt <= $topappapinfo) {
           @app = split /\./, $kk;
           $app = pop @app;
           $t2apinfo = `db2pd -d $db -apinfo $app`;
           print LOG_APINFO "$adate t${topappapinfo}apinfo $t2apinfo";
           }
        }
        }
}
elsif($sortread) {
        foreach $kk (sort{(split /:/, $dapplcnt1{$b})[0] <=> (split /:/, $dapplcnt1{$a})[0]} keys %dapplcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dapplcnt1{$kk})[0])/$sd3);
        print "$adate appl_Rows_rw $kk : $dapplcnt1{$kk} : $gdapplcnt1\n";
        $loopcnt++;
        last if($loopcnt == $apptopcnt);
        if($logappapinfo) {
        if($loopcnt <= $topappapinfo) {
           @app = split /\./, $kk;
           $app = pop @app;
           $t2apinfo = `db2pd -d $db -apinfo $app`;
           print LOG_APINFO "$adate t${topappapinfo}apinfo $t2apinfo";
           }
        }
        }
}
else {
        foreach $kk (sort{(split /:/, $dapplcnt1{$b})[0]+(split /:/, $dapplcnt1{$b})[1]  <=> (split /:/, $dapplcnt1{$a})[0]+(split /:/, $dapplcnt1{$a})[1]} keys %dapplcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dapplcnt1{$kk})[0]+(split /:/, $dapplcnt1{$kk})[1])/$sd3);
        print "$adate appl_Rows_rw $kk : $dapplcnt1{$kk} : $gdapplcnt1\n";
        $loopcnt++;
        last if($loopcnt == $apptopcnt);
        if($logappapinfo) {
        if($loopcnt <= $topappapinfo) {
           @app = split /\./, $kk;
           $app = pop @app;
           $t2apinfo = `db2pd -d $db -apinfo $app`;
           print LOG_APINFO "$adate t${topappapinfo}apinfo $t2apinfo";
           }
        }
        }
}
}

$loopcnt = 0;

print "+" x 100 . "\n";
print "+" x 100 . "\n";
close(LOG_APINFO);

sub do_help { 
    $help = <<EOF; 
usage: perl ./db2inframon_apinfo -d <dbname> -s <appsleepsec,0> -q <read> -w <mod,tran> -e <table,appl> -t <toptab,0> -a <topappl,0> -p <topappllog> -o <directory> [-z:log] [-x:locklog] [-c:applog] [-v:before ver.] [-W:write sort] [-R:read sort] [-O:once a day logging]
help: perl ./db2inframon_apinfo -h 
EOF
    print "$help\n"; 
    exit; 
}


