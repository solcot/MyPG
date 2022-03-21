#!/usr/bin/perl
#use strict;
use warnings;
# ex) perl ./inframon_drpt_full.pl -c 5 -u 2 -t 10 -x ./package.sh -y ./package.sh -n ./package_header.sh -l ./package_log.sh -q 2 -e -o /work3/db2/V11.5.dc_inshome/jhkim/MONITOR/LOG_PERL

use Getopt::Std;
my %options=();
getopts("hc:u:t:x:y:n:l:q:eo:", \%options);

&do_help() if defined $options{h};
defined $options{c} ? my $colnum = $options{c} : exit (print "require -c option... for help -h option...\n");
defined $options{u} ? my $sortcolnum = $options{u} : exit (print "require -u option... for help -h option...\n");
defined $options{t} ? my $topcnt = $options{t} : exit (print "require -t option... for help -h option...\n");
defined $options{x} ? my $ffilenm = $options{x} : exit (print "require -x option... for help -h option...\n");
defined $options{y} ? my $sfilenm = $options{y} : exit (print "require -y option... for help -h option...\n");
defined $options{o} ? my $logfile_dir = $options{o} : exit (print "require -y option... for help -h option...\n");
my $nfilenm = $options{n} if defined $options{n};
my $logyn = $options{l} if defined $options{l};
my $logcnt = $options{q} if defined $options{q};
my $execdeltayn = $options{e} if defined $options{e};
$execdeltayn = 0 if ! $execdeltayn;

my $line;
my @bdat;
my @adat;
my @rdat;
my $Rb;
my $Ra;
my $bkey;
my $akey;
my @tline;
my $i;
my $j;
my $Rr;
my $loopcnt;
my $helpstr;
my $btime;
my $atime;
my $timedelta;
my $numexec;

chomp( $logsdate = `date +"%Y%m%d"` );
$flogfile_dir = $logfile_dir . "/" . substr($logsdate, 0, 6);
`mkdir $flogfile_dir` if !(-d "$flogfile_dir");
$logfile = $flogfile_dir . "/db2drpt_$logsdate.log";
$logfile_apinfo = $flogfile_dir . "/db2drptlog_$logsdate.log";

open STDOUT, ">> $logfile" or die "error $!";
open STDERR, ">> $logfile" or die "error $!";

open(IN, $logfile_dir . "/tmpdrptdate"); $btime = <IN>; close(IN);
open(IN, $logfile_dir . "/tmpdrptdata"); $tmpdrptdata = do{local $/; <IN>}; close(IN);
open(FR, "<", \$tmpdrptdata);
while ($line=<FR>)
{
        $line=~s/^\s+//; push @bdat, [ split(m/\s+/,$line) ];
}
close FR;
#print "$tmpdrptdata\n";

# sleep $sleepsec;

chomp( $atime = `date +'%Y-%m-%d-%H.%M.%S'` );
open(OUT, ">" . $logfile_dir . "/tmpdrptdate"); print OUT "$atime"; close(OUT);
$tmpdrptdata = `ksh $sfilenm`;
open(OUT, ">" . $logfile_dir . "/tmpdrptdata"); print OUT "$tmpdrptdata"; close(OUT);
open(FR, "<", \$tmpdrptdata);
while ($line=<FR>)
{
        $line=~s/^\s+//; push @adat, [ split(m/\s+/,$line) ];
}
close FR;
#print "$tmpdrptdata\n";

$timedelta = &timediff($btime, $atime);

foreach $Rb (@bdat) {
        $bkey = $Rb->[0];
        foreach $Ra (@adat) {
                $akey = $Ra->[0];
                if($bkey eq $akey) {
                        #push @rdat, [ qw( $bkey $Ra->[1]-$Rb->[1] $Ra->[2]-$Rb->[2] $Ra->[3]-$Rb->[3] $Ra->[4]-$Rb->[4] ) ];
                        push @tline, $bkey;
                                                if($execdeltayn) {
                                                        foreach $i (1..$colnum) {
                                                                if($i==1) {
                                                                        $numexec = $Ra->[$i]-$Rb->[$i];
                                                                        push @tline, sprintf("%.2f",$numexec);
                                                                        $numexec = 1 if $numexec==0;

                                                                }
                                                                else {
                                                                        push @tline, sprintf("%.2f",($Ra->[$i]-$Rb->[$i])/$numexec);
                                                                }
                                                        }
                                                }
                                                else {
                                                        foreach $i (1..$colnum) {
                                                                        push @tline, sprintf("%.2f",($Ra->[$i]-$Rb->[$i])/$timedelta);
                                                        }
                                                }
                        push @rdat, [ @tline ];
                        undef @tline;
                }
        }
}

$size = $#rdat +1;
print "**##### Time Delta : $timedelta sec [ $atime ] [- $execdeltayn -] #####**\n";
foreach $i (1..$sortcolnum) {
print "***** [$i]th column sort report *****\n";
if($nfilenm) {
        open FR, "ksh $nfilenm|" || die("Cannot open the file $!");
        while ($line=<FR>)
        {
                print "$line";
        }
        close FR;
}
$loopcnt = 0;
foreach $Rr (sort{$b->[$i] <=> $a->[$i]} @rdat) {
        foreach $j (0..$colnum) {
                printf "%25s",$Rr->[$j];
                system(qq{ksh $logyn "$Rr->[$j]" $atime $logfile_apinfo}) if $j==0 && $logyn && $logcnt>$loopcnt;
        }
        print "\n";
        $loopcnt++;
        last if($loopcnt == $topcnt);
}
print "\n";
last if ($size == 1);
}



sub do_help {
$helpstr = <<EOF;
*** usage: perl ./db2inframon_drpt.pl -c <column count> -u <sort column count> -t <top result> -x <first file name> -y <second file name> [-n <header file name>] [-l <log file name> [-q <log top cnt>]] [-e:execution delta yn] -o <directory>
*** help: perl ./db2inframon_drpt.pl -h
EOF
print "$helpstr\n";
exit;
}

sub timediff {
use Time::Local;
$_[0] =~ s/\s+//; $_[1] =~ s/\s+//;
($year,$month,$day,$hour,$min,$sec) = (substr($_[0],0,4),substr($_[0],5,2),substr($_[0],8,2),substr($_[0],11,2),substr($_[0],14,2),substr($_[0],17,2));
($year2,$month2,$day2,$hour2,$min2,$sec2) = (substr($_[1],0,4),substr($_[1],5,2),substr($_[1],8,2),substr($_[1],11,2),substr($_[1],14,2),substr($_[1],17,2));
$btimesec = timelocal($sec,$min,$hour,$day,$month-1,$year);
$atimesec = timelocal($sec2,$min2,$hour2,$day2,$month2-1,$year2);
$timediff = $atimesec - $btimesec;
return $timediff;
}


