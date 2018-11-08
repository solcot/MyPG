#!/usr/bin/perl 
#use strict;
use warnings; 
# ex) perl ./db2inframon_drpt.pl -c 4 -s 5 -t 10 -x ./aaa.sh -y ./aaa.sh -e -n ./aaa_header.sh

use Getopt::Std;
my %options=();
getopts("hc:s:t:x:y:n:el", \%options); 

&do_help() if defined $options{h};
defined $options{c} ? my $colnum = $options{c} : exit (print "require -c option... for help -h option...\n");
defined $options{s} ? my $sleepsec = $options{s} : exit (print "require -s option... for help -h option...\n");
defined $options{t} ? my $topcnt = $options{t} : exit (print "require -t option... for help -h option...\n");
defined $options{x} ? my $ffilenm = $options{x} : exit (print "require -x option... for help -h option...\n");
defined $options{y} ? my $sfilenm = $options{y} : exit (print "require -y option... for help -h option...\n");
my $nfilenm = $options{n} if defined $options{n};
my $execdeltayn = $options{e} if defined $options{e};
my $logyn = $options{l} if defined $options{e} && defined $options{l};

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

#open FR, "file1.txt" || die("Cannot open the file $!");
open FR, "ksh $ffilenm|" || die("Cannot open the file $!");
chomp( $btime = `date +'%Y-%m-%d-%H.%M.%S'` );
while ($line=<FR>) 
{   
        push @bdat, [ split(m/\s+/,$line) ];
}
close FR;

sleep $sleepsec;

#open FR, "file2.txt" || die("Cannot open the file $!");
open FR, "ksh $sfilenm|" || die("Cannot open the file $!");
chomp( $atime = `date +'%Y-%m-%d-%H.%M.%S'` );
while ($line=<FR>) 
{   
        push @adat, [ split(m/\s+/,$line) ];
}
close FR;

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

print "**##### Time Delta : $timedelta sec [ $atime ] #####**\n";
foreach $i (1..$colnum) {
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
		system(qq{ksh ./aaa_log.sh "$Rr->[$j]" $atime}) if $j==0 && $logyn;
        }
        print "\n";
        $loopcnt++;
        last if($loopcnt == $topcnt);
}
print "\n";
}



sub do_help { 
$helpstr = <<EOF; 
*** usage: perl ./db2inframon_drpt.pl -c <column count> -s <sleep sec> -t <top result> -x <first file name> -y <second file name> [-n <header file name>] [-e:execution delta yn [-l:log yn]] 
*** help: perl ./db2inframon_drpt.pl -h 
EOF
print "$helpstr\n"; 
exit; 
} 

sub timediff {
use Time::Local;
($year,$month,$day,$hour,$min,$sec) = (substr($_[0],0,4),substr($_[0],5,2),substr($_[0],8,2),substr($_[0],11,2),substr($_[0],14,2),substr($_[0],17,2));
($year2,$month2,$day2,$hour2,$min2,$sec2) = (substr($_[1],0,4),substr($_[1],5,2),substr($_[1],8,2),substr($_[1],11,2),substr($_[1],14,2),substr($_[1],17,2));
$btimesec = timelocal($sec,$min,$hour,$day,$month-1,$year);
$atimesec = timelocal($sec2,$min2,$hour2,$day2,$month2-1,$year2);
$timediff = $atimesec - $btimesec;
return $timediff;
}

