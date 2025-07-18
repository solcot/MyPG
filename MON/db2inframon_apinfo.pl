#!/usr/bin/perl
#use strict; 
#use perl ./db2inframon_apinfo.pl -d tdb -s 0 -q 10000000 -w 1000000 -e 10000000 -t 10 -a 0 -p 0 -o /work2/db2/V11.5/jhkim/MONITOR/LOG_PERL -z -x -c -O

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

`db2 connect to $db`;

chomp( $logsdate = `date +"%Y%m%d"` );
$flogfile_dir = $logfile_dir . "/" . substr($logsdate, 0, 6);
`mkdir $flogfile_dir` if !(-d "$flogfile_dir");
$logfile = $flogfile_dir . "/db2inframon_$logsdate.log";
$logfile_apinfo = $flogfile_dir . "/db2apinfo_$logsdate.log";

if($adaylogging) {
if (!(-e "$logfile")) {
#$out = `db2 "select '$db' dbnm,rows_read,rows_modified,rows_inserted,rows_updated,rows_deleted,total_app_commits,int_commits,total_app_rollbacks,int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS,LOCK_ESCALS,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS,TOTAL_HASH_JOINS,HASH_JOIN_OVERFLOWS,current_timestamp ts from table(MON_GET_DATABASE(-2)) with ur"`;
#open(OUT, ">" . $logfile . "_db"); print OUT "$out"; close(OUT);

#$out = `db2 "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50) tabname, sum(rows_read) rr, sum(rows_inserted+rows_updated+rows_deleted) rm, sum(rows_inserted) ri, sum(rows_updated) ru, sum(rows_deleted) rd, sum(table_scans) ts, max(section_exec_with_col_references) sewcr, sum(COALESCE(DATA_OBJECT_L_PAGES,0)) datpag, sum(COALESCE(LOB_OBJECT_L_PAGES,0)) lobpg, sum(COALESCE(INDEX_OBJECT_L_PAGES,0)) idxpg, sum(LOCK_ESCALS) lock_escals,sum(LOCK_WAITS) lock_waits,count(*) pcnt,current_timestamp ts from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
#open(OUT, ">" . $logfile . "_tab"); print OUT "$out"; close(OUT);

#$out = `db2 +w "select executable_id, num_exec_with_metrics, stmt_exec_time, rows_read, rows_modified, rows_returned, total_cpu_time, total_sorts, SORT_OVERFLOWS,LOCK_ESCALS,LOCK_WAITS,DEADLOCKS,LOCK_TIMEOUTS,PACKAGE_SCHEMA,PACKAGE_NAME,EFFECTIVE_ISOLATION,varchar(stmt_text,250) stmt_text,current_timestamp ts from table(mon_get_pkg_cache_stmt(null,null,'<modified_within>1440</modified_within>',-2))"`;
#open(OUT, ">" . $logfile . "_pcache"); print OUT "$out"; close(OUT);

#$out = qx{ db2 prune history `TZ=KST+183; date +%Y%m%d` };  # aix
$out = qx{ db2 prune history `date -d '8 day ago' +%Y%m%d` };  # linux
open(OUT, ">" . $logfile . "_prune"); print OUT "$out"; close(OUT);

$out = qx{ db2pd -d $db -logs |head -25 };
open(OUT, ">" . $logfile . "_logs"); print OUT "$out"; close(OUT);

$out = qx{ (db2audit flush; db2audit archive database $db) };
open(OUT, ">" . $logfile . "_audit"); print OUT "$out"; close(OUT);

#system(qq{ sh ~/IFR/RUNSTATS/thread_main.sh > ~/IFR/RUNSTATS/thread_main.sh.log 2>&1 });
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
#$bsnapdbts = `db2 -x "values current timestamp with ur"`;
open(IN, $logfile_dir . "/tmpdbts"); $bsnapdbts = <IN>; close(IN);
if($before) {
#   $tmpdb = `db2 -x "select rows_read,rows_deleted+rows_inserted+rows_updated,commit_sql_stmts+int_commits+rollback_sql_stmts+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,0,0 from sysibmadm.snapdb with ur"`;
   open(IN, $logfile_dir . "/tmpdb"); $tmpdb = <IN>; close(IN);
} else {
#   $tmpdb = `db2 -x "select rows_read,rows_modified,total_app_commits+int_commits+total_app_rollbacks+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS from table(MON_GET_DATABASE(-2)) with ur"`;
   open(IN, $logfile_dir . "/tmpdb"); $tmpdb = <IN>; close(IN);
}
$tmpdb =~ s/^\s+|\s+$//g;
($bdbcnt1, $bdbcnt2, $bdbcnt3, $bdbcnt4, $bdbcnt5, $bdbcnt6, $bdbcnt7, $bdbcnt8, $bdbcnt9, $bdbcnt10, $bdbcnt11, $bdbcnt12, $bdbcnt13,$bdbcnt14,$bdbcnt15,$bdbcnt16,$bdbcnt17, $bdbcnt18,$bdbcnt19,$bdbcnt20,$bdbcnt21,$bdbcnt22, $bdbcnt23,$bdbcnt24,$bdbcnt25,$bdbcnt26,$bdbcnt27,$bdbcnt28,$bdbcnt29,$bdbcnt30,$bdbcnt31) = split /\s+/, $tmpdb;
#print "$bdbcnt1, $bdbcnt2, $bdbcnt3\n";
## snaptab
if($tabtopcnt > 0) {
#$bsnaptabts = `db2 -x "values current timestamp with ur"`;
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
{ $bsnapapplts = `db2 -x "values current timestamp with ur"`; } else
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
$asnapdbts = `db2 -x "values current timestamp with ur"`;
open(OUT, ">" . $logfile_dir . "/tmpdbts"); print OUT "$asnapdbts"; close(OUT);
if($before) {
   $tmpdb = `db2 -x "select rows_read,rows_deleted+rows_inserted+rows_updated,commit_sql_stmts+int_commits+rollback_sql_stmts+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,0,0,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS,COMMIT_SQL_STMTS,INT_COMMITS,ROLLBACK_SQL_STMTS,INT_ROLLBACKS,LOCK_ESCALS,TOTAL_HASH_JOINS,HASH_JOIN_OVERFLOWS 
   ,0,rows_inserted,rows_updated,rows_deleted,rows_selected,ELAPSED_EXEC_TIME_MS,COMMIT_SQL_STMTS+ROLLBACK_SQL_STMTS
   from sysibmadm.snapdb with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
} else {
   $tmpdb = `db2 -x "select rows_read,rows_deleted+rows_inserted+rows_updated,total_app_commits+int_commits+total_app_rollbacks+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS, total_app_commits,int_commits,total_app_rollbacks,int_rollbacks,LOCK_ESCALS,TOTAL_HASH_JOINS,HASH_JOIN_OVERFLOWS 
   ,rows_modified,rows_inserted,rows_updated,rows_deleted,rows_returned,TOTAL_RQST_TIME,RQSTS_COMPLETED_TOTAL
   from table(MON_GET_DATABASE(-2)) with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
}
$tmpdb =~ s/^\s+|\s+$//g;
($adbcnt1, $adbcnt2, $adbcnt3, $adbcnt4, $adbcnt5, $adbcnt6, $adbcnt7, $adbcnt8, $adbcnt9, $adbcnt10, $adbcnt11, $adbcnt12, $adbcnt13,$adbcnt14,$adbcnt15,$adbcnt16,$adbcnt17, $adbcnt18,$adbcnt19,$adbcnt20,$adbcnt21,$adbcnt22, $adbcnt23,$adbcnt24,$adbcnt25,$adbcnt26,$adbcnt27,$adbcnt28,$adbcnt29,$adbcnt30,$adbcnt31) = split /\s+/, $tmpdb;
## snaptab
if($tabtopcnt > 0) {
$asnaptabts = `db2 -x "values current timestamp with ur"`;
open(OUT, ">" . $logfile_dir . "/tmptabts"); print OUT "$asnaptabts"; close(OUT);
if($before) {
   #$atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), trim(char(sum(rows_read))) || ':' || trim(char(sum(rows_written))) || ':' || trim(char(0)) || ':' || trim(char(0)) || ':' || trim(char(sum(COALESCE(DATA_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(LOB_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(INDEX_OBJECT_PAGES,0)))) || ':' || trim(char(count(*))) || ':' || trim(char(0)) || ':' || trim(char(0)) from sysibmadm.snaptab where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;  # <V9.7 
   $atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), trim(char(sum(rows_read))) || ':' || trim(char(sum(rows_inserted+rows_updated+rows_deleted))) || ':' || varchar(sum(table_scans)) || ':' || trim(char(0)) || ':' || trim(char(sum(COALESCE(DATA_OBJECT_L_PAGES,0)))) || ':' || trim(char(sum(COALESCE(LOB_OBJECT_L_PAGES,0)))) || ':' || trim(char(sum(COALESCE(INDEX_OBJECT_L_PAGES,0)))) || ':' || trim(char(count(*))) || ':' || trim(char(0)) || ':' || trim(char(0)) from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;  # =V9.7
   open(OUT, ">" . $logfile_dir . "/tmptab"); print OUT "$atabcnt1"; close(OUT);
} else {
   $atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','') || '.' || replace(tabname,' ',''),50), varchar(sum(rows_read)) || ':' || varchar(sum(rows_inserted+rows_updated+rows_deleted)) || ':' || varchar(sum(table_scans)) || ':' || varchar(max(section_exec_with_col_references)) || ':' || varchar(sum(COALESCE(DATA_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(LOB_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(INDEX_OBJECT_L_PAGES,0))) || ':' || varchar(count(*)) || ':' || varchar(sum(LOCK_ESCALS)) || ':' || varchar(sum(LOCK_WAITS)) from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmptab"); print OUT "$atabcnt1"; close(OUT);
}
%atabcnt1s = split /\s+/, $atabcnt1;
}
## snapappl
if($apptopcnt > 0) {
$asnapapplts = `db2 -x "values current timestamp with ur"`;
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
$ddbcnt1 = int(($adbcnt1-$bdbcnt1)/$snapdbtimediff);
$gddbcnt1 = "#" x ($ddbcnt1/$sd1);
$ddbcnt2 = int(($adbcnt2-$bdbcnt2)/$snapdbtimediff);
$gddbcnt2 = "#" x ($ddbcnt2/$sd2);
$ddbcnt3 = int(($adbcnt3-$bdbcnt3)/$snapdbtimediff);
$gddbcnt3 = "#" x ($ddbcnt3/$sd2);
$ddbcnt4 = sprintf("%.2f", ($adbcnt4-$bdbcnt4)/$snapdbtimediff);
$ddbcnt5 = int(($adbcnt5-$bdbcnt5)/$snapdbtimediff);
$ddbcnt6 = int(($adbcnt6-$bdbcnt6)/$snapdbtimediff);
$ddbcnt7 = int(($adbcnt7-$bdbcnt7)/$snapdbtimediff);
$ddbcnt8 = int(($adbcnt8-$bdbcnt8)/$snapdbtimediff);
$ddbcnt9 = int(($adbcnt9-$bdbcnt9)/$snapdbtimediff);
$ddbcnt10 = int(($adbcnt10-$bdbcnt10)/$snapdbtimediff);
$ddbthrouput = $ddbcnt8 + $ddbcnt9 + $ddbcnt10; #$ddbcnt5 + $ddbcnt6 - $ddbcnt7;
$ddbcnt11 = int(($adbcnt11-$bdbcnt11)/$snapdbtimediff);
$ddbcnt12 = int(($adbcnt12-$bdbcnt12)/$snapdbtimediff);
$ddbcnt13 = sprintf("%.2f", ($adbcnt13-$bdbcnt13)/$snapdbtimediff);
$ddbcnt14 = sprintf("%.2f", ($adbcnt14-$bdbcnt14)/$snapdbtimediff);
$ddbcnt15 = sprintf("%.2f", ($adbcnt15-$bdbcnt15)/$snapdbtimediff);
$ddbcnt16 = sprintf("%.2f", ($adbcnt16-$bdbcnt16)/$snapdbtimediff);
$ddbcnt17 = sprintf("%.2f", ($adbcnt17-$bdbcnt17)/$snapdbtimediff);
$ddbcnt18 = int(($adbcnt18-$bdbcnt18)/$snapdbtimediff);
$ddbcnt19 = int(($adbcnt19-$bdbcnt19)/$snapdbtimediff);
$ddbcnt20 = int(($adbcnt20-$bdbcnt20)/$snapdbtimediff);
$ddbcnt21 = int(($adbcnt21-$bdbcnt21)/$snapdbtimediff);
$ddbcnt22 = sprintf("%.2f", ($adbcnt22-$bdbcnt22)/$snapdbtimediff);
$ddbcnt23 = sprintf("%.2f", ($adbcnt23-$bdbcnt23)/$snapdbtimediff);
$ddbcnt24 = sprintf("%.2f", ($adbcnt24-$bdbcnt24)/$snapdbtimediff);

$ddbcnt25 = sprintf("%.2f", ($adbcnt25-$bdbcnt25)/$snapdbtimediff);
$ddbcnt26 = sprintf("%.2f", ($adbcnt26-$bdbcnt26)/$snapdbtimediff);
$ddbcnt27 = sprintf("%.2f", ($adbcnt27-$bdbcnt27)/$snapdbtimediff);
$ddbcnt28 = sprintf("%.2f", ($adbcnt28-$bdbcnt28)/$snapdbtimediff);
$ddbcnt29 = sprintf("%.2f", ($adbcnt29-$bdbcnt29)/$snapdbtimediff);

$ddbcnt30 = $adbcnt30-$bdbcnt30;
$ddbcnt31 = $adbcnt31-$bdbcnt31;
$avgrqsttime = sprintf("%.2f", $ddbcnt30/($ddbcnt31+1));

#print "$ddbcnt1, $ddbcnt2, $ddbcnt3\n";
## snaptab
if($tabtopcnt > 0) {
%dtabcnt1 = ();
foreach $k1 (keys %atabcnt1s) {
        foreach $k2 (keys %btabcnt1s) {
                                if($k1 eq $k2) {
                                @atabs = split /:/, $atabcnt1s{$k1};
                                @btabs = split /:/, $btabcnt1s{$k2};
                                $dtabcnt1{$k1} = int(($atabs[0] - $btabs[0])/$snaptabtimediff) . ":" . int(($atabs[1] - $btabs[1])/$snaptabtimediff) . ":" . sprintf("%.2f",($atabs[2] - $btabs[2])/$snaptabtimediff) . ":" . sprintf("%.2f",($atabs[3] - $btabs[3])/$snaptabtimediff) . ":" . int($atabs[4]) . ":" . int($atabs[5]) . ":" . int($atabs[6]) . ":" . int($atabs[7]) . ":" . sprintf("%.2f",($atabs[8] - $btabs[8])/$snaptabtimediff) . ":" . sprintf("%.2f",($atabs[9] - $btabs[9])/$snaptabtimediff);
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
   $lock = `db2 -x "select a.agent_id_holding_lk, a.tabname from sysibmadm.snaplockwait a
        where a.agent_id_holding_lk not in (select agent_id from sysibmadm.snaplockwait) group by a.agent_id_holding_lk, a.tabname with ur"`;
} else {
   $lock = `db2 -x "select a.hld_application_handle, a.tabname from sysibmadm.mon_lockwaits a
        where a.hld_application_handle not in (select req_application_handle from sysibmadm.mon_lockwaits) group by a.hld_application_handle, a.tabname with ur"`;
}
@locks = split /\s+/, $lock;
## connection
if($before) {
   $conn = `db2 -x "select APPLS_CUR_CONS, APPLS_IN_DB2, LOCKS_WAITING,ACTIVE_SORTS,ACTIVE_HASH_JOINS from sysibmadm.snapdb with ur"`;
} else {
   $conn = `db2 -x "select APPLS_CUR_CONS, APPLS_IN_DB2, NUM_LOCKS_WAITING,ACTIVE_SORTS,ACTIVE_HASH_JOINS from table(MON_GET_DATABASE(-2)) with ur"`;
}
@conns = split /\s+/, $conn;

################ print
system(qq/ db2 -x "select max(application_handle)::varchar(10),stmtid,count(*),max(rows_read),max(total_act_time),max(lock_wait_time),sum(total_cpu_time),sum(total_disp_run_queue_time),max(activity_state) from table(mon_get_activity(null,-1)) where application_handle <> mon_get_application_handle() group by stmtid" /);
## timediff
print "dbsecdiff: $snapdbtimediff tabsecdiff: $snaptabtimediff applsecdiff: $snapappltimediff\n";
## cpu
#print "$adate CPU $cpu < R: $rq B: $bq P: $pq W: $wq AVM: $avm FRE: $fre FI: $fi FO: $fo PI: $pi PO: $po IN: $in SC: $sc CS: $cs U: $us S: $sy W: $wa I: $id >\n";  #aix
print "$adate CPU $cpu < R: $rq B: $bq PG: $swap FRE: $free BUFF: $buff CACHE: $cache FI: $fi FO: $fo PI: $pi PO: $po IN: $in CS: $cs U: $us S: $sy W: $wa I: $id >\n"; #linux
print "$adate CPU: $gcpu\n";
## lock
print "$adate Lock_holder : @locks\n";
if($loglockapinfo){
while(defined($flockap = shift @locks)) {
        if(($flockap+0) eq $flockap) {
           $flockapinfo = `db2pd -d $db -apinfo $flockap|head -150`;
           print LOG_APINFO "$adate flockapinfo $flockapinfo";
        }
}
}
## snapdb
print "$adate Rqsttime $avgrqsttime ( $ddbcnt30 $ddbcnt31 ) : : \n"; #avgrequesttime, requesttime, requesttotal^M
print "$adate Lock $ddbcnt22 $ddbcnt13 $ddbcnt14 $ddbcnt15 : $conns[3]\n"; #escal,dead,timeout,wait,waiting
print "$adate Sort $ddbcnt16 $ddbcnt17 : $conns[4]\n"; #total,overflow,active
print "$adate HSjoin $ddbcnt23 $ddbcnt24 : $conns[5]\n"; #total,overflow,active
print "$adate CPU_time $ddbcnt11 $ddbcnt12\n"; #time,latchwait
print "$adate Connection $ddbcnt4 : $conns[1] $conns[2]\n"; #newcon,total,active
print "$adate Trans $ddbcnt3 ( $ddbcnt18 $ddbcnt19 $ddbcnt20 $ddbcnt21 ) > $gddbcnt3\n"; #tran,commit,intcomm,rollback,introll
print "$adate Throughput $ddbthrouput ( $ddbcnt8 $ddbcnt9 $ddbcnt10 ) ( $ddbcnt5 $ddbcnt6 $ddbcnt7 )\n"; #S+IUD+ddl,select,iud,ddl,static,dynamic,failed
print "$adate Rows_read $ddbcnt1 ( $ddbcnt29 ) > $gddbcnt1\n"; #rows_read ( rows_returned )
print "$adate Rows_write $ddbcnt2 ( $ddbcnt26 $ddbcnt27 $ddbcnt28 ) $ddbcnt25 > $gddbcnt2\n"; #i+u+d ( i u d) rows_modified
print "-" x 20 . "\n";
## snaptab : read,write,tbscan,qrycnt,data,lob,index,pcnt,escal,wait
if($tabtopcnt > 0) {
if($sortwrite) {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[1]  <=> (split /:/, $dtabcnt1{$a})[1]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[1])/$sd3);
        print "$adate Tab_Rows_rw $kk : $dtabcnt1{$kk} : $gdtabcnt1\n";
        $loopcnt++;
        last if($loopcnt == $tabtopcnt);
        }
}
elsif($sortread) {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[0] <=> (split /:/, $dtabcnt1{$a})[0]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[0])/$sd3);
        print "$adate Tab_Rows_rw $kk : $dtabcnt1{$kk} : $gdtabcnt1\n";
        $loopcnt++;
        last if($loopcnt == $tabtopcnt);
        }
}
else {
        foreach $kk (sort{(split /:/, $dtabcnt1{$b})[0]+(split /:/, $dtabcnt1{$b})[1]  <=> (split /:/, $dtabcnt1{$a})[0]+(split /:/, $dtabcnt1{$a})[1]} keys %dtabcnt1) {
        $gdtabcnt1 = "#" x (((split /:/, $dtabcnt1{$kk})[0]+(split /:/, $dtabcnt1{$kk})[1])/$sd3);
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
close(LOG_APINFO);

sub do_help { 
    $help = <<EOF; 
usage: perl ./db2inframon_apinfo -d <dbname> -s <appsleepsec,0> -q <read> -w <mod,tran> -e <table,appl> -t <toptab,0> -a <topappl,0> -p <topappllog> -o <directory> [-z:log] [-x:locklog] [-c:applog] [-v:before ver.] [-W:write sort] [-R:read sort] [-O:once a day logging]
help: perl ./db2inframon_apinfo -h 
EOF
    print "$help\n"; 
    exit; 
}


