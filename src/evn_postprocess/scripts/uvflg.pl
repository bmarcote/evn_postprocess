#!/usr/bin/perl -w
# program to read FS logs and extract flagging information.
# Cormac Reynolds:  Oct 2002 - original program.
#                   Jul 2003 - added -int flag. Fixed bug in reading interval.

use Getopt::Long;
use POSIX;
use PGPLOT;
use strict;
my ($infile,$interval,$state,$telescope,$reason,$flagging,$time,$starttime);
my ($endtime,$expname,$flagfile,$intall);

# the current version of the code.
my ($version) = '7 July 2003';

GetOptions ('int=f'=>\$intall) ;

usage ();

# get the name of the input file
chomp ($infile=$ARGV[0]) if (defined $ARGV[0]);
unless (defined ($infile) && -e $infile) {
  print "Enter the input file name:\n";
  chomp ($infile = <STDIN>);
}
open (INFILE,"$infile") || die "can\'t open file $infile: $!\n";
print "$infile\n";

# from the filename, derive the experiment and station names.
$telescope = substr($infile, -6, 2);
$telescope =~ tr/a-z/A-Z/;
$expname = substr($infile, 0, -6);

$flagfile = substr($infile, 0, -4);
$flagfile = $flagfile . ".uvflgfs";
open (FLAGFILE,">$flagfile") || die "can\'t open file $flagfile: $!\n";

# write the header information.
header();

# toggle when flagging is in progress.
$flagging = 0;
if (!defined $intall) {
  $interval = 0;
}else {
  $interval = $intall;
}

#read in the data
while (<INFILE>) {
  chomp;
  # remove any comments
  s/".*$//g;

  # Get the flagging interval
  if (/flagr,/ && !defined $intall) {
      $interval = getinterval($_);
      print "line $., interval=$interval\n";
  }

  if (/#flagr\/antenna,/ && $interval>0) {
    $time = $`;
    $state = $';

    $time =~ s/#.*$//;
    if ($state =~ /\w\w\w-source/ && !$flagging) {
      $flagging = 1;
      $starttime = convtime($time);
      $reason = 'Antenna off source';
    }elsif ($state =~ /acquired/ && $flagging) {
      $flagging = 0;
      $endtime = convtime($time);
      writeflags ($starttime,$endtime,$reason);
    }

  }

}
close (INFILE);
close (FLAGFILE);

##################################################
sub header {
  print FLAGFILE <<EOF
! The following is flagging information for $telescope in experiment $expname, 
! extracted from $infile by uvflg.pl version "$version".
opcode='FLAG'
dtimrang = 1   timeoff=0
EOF

}

##################################################
sub getinterval {
  my ($interval) = $_[0];

  $interval =~ s/^.*flagr,//g;
  $interval = abs($interval);
  # convert to seconds
  $interval = $interval / 100;

  return $interval;
}

##################################################
sub convtime {
  my ($time) = $_[0];

  $time =~ /(\d\d\d\d)\.(\d\d\d)\.(\d\d):(\d\d):(\d\d\.\d\d)/;
  my ($year) = $1;
  my ($day) = $2;
  my ($hour) = $3;
  my ($min) = $4;
  my ($sec) = $5;

  # UVFLG only takes integer seconds.
  $sec = floor($sec);
  
  # round up to the next second if the flagging is ending
  $sec = $sec + 1 - $flagging;

  # subtract the flagging interval from the current time - iff the flagging is
  # starting
  $sec = $sec - $interval*$flagging;

  $sec = sprintf("%02d", $sec);

  # the following doesn't work for midnight on New Year's Eve (UVFLG only
  # accepts the day, not the year)
  if ($sec < 0) {
    $sec = $sec + 60;
    $min = $min - 1;
    $min = sprintf("%02d", $min);
    if ($min < 0) {
      $min = $min + 60;
      $hour = $hour - 1;
      $hour = sprintf("%02d", $hour);
      if ($hour < 0) {
        $hour = $hour + 24;
        $day = $day - 1;
        $day = sprintf("%03d", $day);
        if ($day < 1) {
          $day = '001'; $hour = '00'; $min = '00'; $sec = '00';
        }
      }
    }
  }

  my ($uvflgtime) = ($day . "," . $hour . "," . $min . "," . $sec) ;

  return $uvflgtime;
}

##################################################
sub writeflags {
  my ($starttime,$endtime,$reason) = @_;

  print FLAGFILE "ant_name=\'$telescope\' timerang=$starttime, $endtime",
                  " reason=\'$reason\' \/\n";

}


sub usage {
  print <<EOF
-------------------------------------------------------------------------------
Usage: uvflg.pl [options] logfile

this program is to produce flagging files from the FS telescope logs for EVN
experiments.  Current version is $version.

Options: 
-int    set the flagr polling interval manually. Normally this number is read
            from the log, but can be set manually where the number in the log
            is incorrect.

Please report any bugs to Cormac Reynolds (reynolds\@jive.nl).

-------------------------------------------------------------------------------

EOF
    ;
}
