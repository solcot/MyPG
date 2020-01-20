#!/usr/bin/perl
#use strict; 
#use perl ./db2inframon_apinfo.pl -d sample -s 5 -q 10000000 -w 1000000 -e 10000000 -t 10 -a 5 -p 2 -o /work3/db2/V11.5.dc_inshome/jhkim/MONITOR/LOG_PERL -z -x -c 

use Getopt::Std; 
 
my %options=(); 
getopts("hd:s:q:w:e:t:a:p:o:zxcvWR", \%options); 
 
if($options{h}) { do_help(); } 
if($options{d}) { $db = $options{d}; } else { print "require -d option... for help -h option...\n"; exit; } 
if($options{s} or $options{s} == 0) { $scnt = $options{s}; } else { print "require -s option... for help -h option...\n"; exit; } 
if($options{q}) { $sd1 = $options{q}; } else { print "require -q option... for help -h option...\n"; exit; } 
if($options{w}) { $sd2 = $options{w}; } else { print "require -w option... for help -h option...\n"; exit; } 
if($options{e}) { $sd3 = $options{e}; } else { print "require -e option... for help -h option...\n"; exit; } 
if($options{t} or $options{t} == 0) { $tabtopcnt = $options{t}; } else { print "require -t option... for help -h option...\n"; exit; } 
if($options{a} or $options{a} == 0) { $apptopcnt = $options{a}; } else { print "require -a option... for help -h option...\n"; exit; } 
if($options{p}) { $topappapinfo = $options{p}; } else { print "require -p option... for help -h option...\n"; exit; } 
if($options{o}) { $logfile_dir = $options{o}; } else { print "require -o option... for help -h option...\n"; exit; } 

if($options{z}) { $log = $options{z}; } 
if($options{x}) { $loglockapinfo = $options{x}; } 
if($options{c}) { $logappapinfo = $options{c}; } 
if($options{v}) { $before = $options{v}; } 

if($options{W}) { $sortwrite = $options{W}; } 
if($options{R}) { $sortread = $options{R}; } 


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

# once a day logging =======================
if (!(-e "$logfile")) {
$out = `db2 "select '$db' dbnm,rows_read,rows_modified,rows_inserted,rows_updated,rows_deleted,total_app_commits,int_commits,total_app_rollbacks,int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS,current_timestamp ts from table(MON_GET_DATABASE(-2)) with ur"`;
open(OUT, ">" . $logfile . "_db"); print OUT "$out"; close(OUT);
$out = `db2 "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50) tabname, sum(rows_read) rr, sum(rows_inserted+rows_updated+rows_deleted) rm, sum(rows_inserted) ri, sum(rows_updated) ru, sum(rows_deleted) rd, sum(table_scans) ts, max(section_exec_with_col_references) sewcr, sum(COALESCE(DATA_OBJECT_L_PAGES,0)) datpag, sum(COALESCE(LOB_OBJECT_L_PAGES,0)) lobpg, sum(COALESCE(INDEX_OBJECT_L_PAGES,0)) idxpg, count(*) pcnt,current_timestamp ts from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
open(OUT, ">" . $logfile . "_tab"); print OUT "$out"; close(OUT);
$out = `db2 +w "select executable_id, num_exec_with_metrics, stmt_exec_time, rows_read, rows_modified, rows_returned, total_cpu_time, total_sorts, SORT_OVERFLOWS, varchar(stmt_text,250) stmt_text,current_timestamp ts from table(mon_get_pkg_cache_stmt(null,null,'<modified_within>720</modified_within>',-2))"`;
open(OUT, ">" . $logfile . "_pcache"); print OUT "$out"; close(OUT);
}
# ==========================================

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
($bdbcnt1, $bdbcnt2, $bdbcnt3, $bdbcnt4, $bdbcnt5, $bdbcnt6, $bdbcnt7, $bdbcnt8, $bdbcnt9, $bdbcnt10, $bdbcnt11, $bdbcnt12, $bdbcnt13,$bdbcnt14,$bdbcnt15,$bdbcnt16,$bdbcnt17, $bdbcnt18,$bdbcnt19,$bdbcnt20,$bdbcnt21 ) = split /\s+/, $tmpdb;
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
   $tmpdb = `db2 -x "select rows_read,rows_deleted+rows_inserted+rows_updated,commit_sql_stmts+int_commits+rollback_sql_stmts+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,0,0,0,0,0,0,0,0,0,0,0 from sysibmadm.snapdb with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
} else {
   $tmpdb = `db2 -x "select rows_read,rows_modified,total_app_commits+int_commits+total_app_rollbacks+int_rollbacks,total_cons,STATIC_SQL_STMTS,DYNAMIC_SQL_STMTS,FAILED_SQL_STMTS,SELECT_SQL_STMTS,UID_SQL_STMTS,DDL_SQL_STMTS,TOTAL_CPU_TIME,TOTAL_EXTENDED_LATCH_WAITS,DEADLOCKS,LOCK_TIMEOUTS,LOCK_WAITS,TOTAL_SORTS,SORT_OVERFLOWS, total_app_commits,int_commits,total_app_rollbacks,int_rollbacks from table(MON_GET_DATABASE(-2)) with ur"`;
   open(OUT, ">" . $logfile_dir . "/tmpdb"); print OUT "$tmpdb"; close(OUT);
}
$tmpdb =~ s/^\s+|\s+$//g;
($adbcnt1, $adbcnt2, $adbcnt3, $adbcnt4, $adbcnt5, $adbcnt6, $adbcnt7, $adbcnt8, $adbcnt9, $adbcnt10, $adbcnt11, $adbcnt12, $adbcnt13,$adbcnt14,$adbcnt15,$adbcnt16,$adbcnt17, $adbcnt18,$adbcnt19,$adbcnt20,$adbcnt21) = split /\s+/, $tmpdb;
## snaptab
if($tabtopcnt > 0) {
$asnaptabts = `db2 -x "values current timestamp with ur"`;
open(OUT, ">" . $logfile_dir . "/tmptabts"); print OUT "$asnaptabts"; close(OUT);
if($before) {
   $atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','')||'.'||replace(tabname,' ',''),50), trim(char(sum(rows_read))) || ':' || trim(char(sum(rows_written))) || ':' || trim(char(0)) || ':' || trim(char(0)) || ':' || trim(char(sum(COALESCE(DATA_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(LOB_OBJECT_PAGES,0)))) || ':' || trim(char(sum(COALESCE(INDEX_OBJECT_PAGES,0)))) || ':' || trim(char(count(*)))  from sysibmadm.snaptab where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;   
   open(OUT, ">" . $logfile_dir . "/tmptab"); print OUT "$atabcnt1"; close(OUT);
} else {
   $atabcnt1 = `db2 -x "select varchar(replace(tabschema,' ','') || '.' || replace(tabname,' ',''),50), varchar(sum(rows_read)) || ':' || varchar(sum(rows_inserted+rows_updated+rows_deleted)) || ':' || varchar(sum(table_scans)) || ':' || varchar(max(section_exec_with_col_references)) || ':' || varchar(sum(COALESCE(DATA_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(LOB_OBJECT_L_PAGES,0))) || ':' || varchar(sum(COALESCE(INDEX_OBJECT_L_PAGES,0))) || ':' || varchar(count(*)) from table(MON_GET_TABLE(null,null,-2)) where tabschema not like 'SYS%' and tabschema not like 'IDBA%' group by tabschema, tabname with ur"`;
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
#print "$ddbcnt1, $ddbcnt2, $ddbcnt3\n";
## snaptab
if($tabtopcnt > 0) {
%dtabcnt1 = ();
foreach $k1 (keys %atabcnt1s) {
        foreach $k2 (keys %btabcnt1s) {
                                if($k1 eq $k2) {
                                @atabs = split /:/, $atabcnt1s{$k1};
                                @btabs = split /:/, $btabcnt1s{$k2};
                                $dtabcnt1{$k1} = int(($atabs[0] - $btabs[0])/$snaptabtimediff) . ":" . int(($atabs[1] - $btabs[1])/$snaptabtimediff) . ":" . sprintf("%.2f",($atabs[2] - $btabs[2])/$snaptabtimediff) . ":" . sprintf("%.2f",($atabs[3] - $btabs[3])/$snaptabtimediff) . ":" . int($atabs[4]) . ":" . int($atabs[5]) . ":" . int($atabs[6]) . ":" . int($atabs[7]);
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
$vmstat = `vmstat -I 1 2 |tail -1`;
$vmstat =~ s/^\s+|\s+$//g;
#($rq,$bq,$pi,$po,$us,$sy,$id,$wa) = (split /\s+/, $vmstat)[0,1,6,7,-5,-4,-3,-2];  # linux
($rq,$bq,$avm,$fre,$fi,$fo,$pi,$po,$in,$sc,$cs,$us,$sy,$id,$wa) = (split /\s+/, $vmstat)[0,1,3,4,5,6,7,8,11,12,13,14,15,16,17];  # > aix 5.3
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
   $conn = `db2 -x "select APPLS_CUR_CONS, APPLS_IN_DB2, 0,0 from sysibmadm.snapdb with ur"`;
} else {
   $conn = `db2 -x "select APPLS_CUR_CONS, APPLS_IN_DB2, NUM_LOCKS_WAITING,ACTIVE_SORTS from table(MON_GET_DATABASE(-2)) with ur"`;
}
@conns = split /\s+/, $conn;

################ print
## timediff
print "dbsecdiff: $snapdbtimediff tabsecdiff: $snaptabtimediff applsecdiff: $snapappltimediff\n";
## cpu
$avmm = int($avm*4/1024);
$frem = int($fre*4/1024);
print "$adate CPU $cpu < R: $rq B: $bq AVM: $avmm FRE: $frem FI: $fi FO: $fo PI: $pi PO: $po IN: $in SC: $sc CS: $cs U: $us S: $sy W: $wa I: $id > : $gcpu\n";
## lock
print "$adate Lock_holder : @locks\n";
if($loglockapinfo){
while(defined($flockap = shift @locks)) {
        if(($flockap+0) eq $flockap) {
           $flockapinfo = `db2pd -d $db -apinfo $flockap`;
           print LOG_APINFO "$adate flockapinfo $flockapinfo";
        }
}
}
## snapdb
print "$adate Lock $ddbcnt13 $ddbcnt14 $ddbcnt15 : $conns[3]\n";
print "$adate Sort $ddbcnt16 $ddbcnt17 : $conns[4]\n";
print "$adate CPU_time $ddbcnt11 : $ddbcnt12\n";
print "$adate Connection $conns[1] $conns[2] : $ddbcnt4\n";
print "$adate Trans $ddbcnt3 : $ddbcnt18 $ddbcnt19 $ddbcnt20 $ddbcnt21 : $gddbcnt3\n";
print "$adate Throughput $ddbthrouput : $ddbcnt5 $ddbcnt6 $ddbcnt7 : $ddbcnt8 $ddbcnt9 $ddbcnt10\n";
print "$adate Rows_read $ddbcnt1 : $gddbcnt1\n";
print "$adate Rows_write $ddbcnt2 : $gddbcnt2\n";
print "-" x 20 . "\n";
## snaptab
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
## snapappl
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
usage: perl ./db2inframon_apinfo -d <dbname> -s <appsleepsec,0> -q <read> -w <mod,tran> -e <table,appl> -t <toptab,0> -a <topappl,0> -p <topappllog> -o <directory> [-z:log] [-x:locklog] [-c:applog] [-v:before ver.] [-W:write sort] [-R:read sort]
help: perl ./db2inframon_apinfo -h 
EOF
    print "$help\n"; 
    exit; 
}

