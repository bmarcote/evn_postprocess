#!/usr/bin/env perl 
# program to read amplitude calibration table written by listr and produce some
# statistics
# Cormac Reynolds:  Mar 2004 - original program.
# Bob Campbell: Apr2014 - added extra regexp checks for changed AMP2.TXT format
#                           AIPS 31dec13

# first add the perlmod directory to @INC
BEGIN{
  $perlmod = $0;
  $perlmod =~ s/\/[^\/]*?$//;
  unless ($perlmod =~ /\//) {
    $perlmod = '.';
  }
  $perlmod .= '/perlmod';
  unshift @INC, $perlmod ;
}
use Getopt::Std;
#use PGPLOT;
use crfuncs;
use strict;
use POSIX;
use vars qw($opt_o);

usage();

getopts ('o');

# get the name of the input file
#chomp ($infile=$ARGV[0]) if (defined $ARGV[0]);
unless (@ARGV) {
  print "Enter the input file name(s) (^D to finish):\n";
  chomp (@ARGV = <STDIN>);
  #die "Cannot find input file: $infile";
}

open (INFILE, "$ARGV[0]") || die "can\'t open file $ARGV[0]: $!\n";
#$ARGV[0] =~ /(.+)_CALIB/i;
$ARGV[0] =~ /(.+)\.TXT/i;
my ($outfile) = $1;
if ($outfile) {
  $outfile .= ".ampcal" ;
}else {
  $outfile = "stats.ampcal";
}
$outfile =~ s/.*\///;
print "outfile=$outfile\n";
  

# get the antenna names from the dtsum file
unless ($ARGV[0] =~ /\//) {
  $ARGV[0] = './' . $ARGV[0];
}
$ARGV[0] =~ /.*\//;
my ($dtsumpath) = $&;
my ($dtsumfile) = $';
if ($opt_o) {
  $dtsumfile =~ s/_CALIB_AMP2.TXT$//;
}else {
  $dtsumfile =~ s/_[^_]*?_CALIB_AMP2.TXT$//;
}
$dtsumfile .= '.DTSUM';
$dtsumfile = $dtsumpath . $dtsumfile;
print "dtsumfile = $dtsumfile\n";

# read in the .dtsum to get the translation from antenna number to antenna name
my (%antnum) = ();
if (-e $dtsumfile) {
  open (DTSUMFILE, "$dtsumfile") || die "can\'t open file $dtsumfile: $!\n";
  my ($antstart) = 0;
  DTSUM: while (<DTSUMFILE>) {
    chomp;
    s/^\s+//;
    if (/Antennas/i) { $antstart = 1; next DTSUM}
    if (/SUMMARY/i) { $antstart = 0; last DTSUM}

    my ($line) = $_;
    if ($antstart) {
      if ($line =~ /:/) {
        # DTSUM format 31DEC06 and earlier
        my (@antline) = split (/\s+/, $line);
        foreach my $antline (@antline) {
          $antnum{ (split(/:/, $antline))[0] } = (split(/:/, $antline))[1]
        }
      } else {
        # DTSUM format 31DEC07 and later
        my @match = /\s*(\d+)\s+\(\s*(\w+)\s*\)/g, $line;
        for (my $i=0; $i < $#match; $i += 2) {
          $antnum{$match[$i]} = $match[$i+1]
        }
      }
    }
  }
}else {
  print "DTSUM file not found\n"
}

foreach my $key (sort {$a <=> $b} keys %antnum) {
  print "$key=$antnum{$key}\n"
}
      
# get the observing frequency from the .SCAN file
$ARGV[0] =~ /.*\//;
my ($scanpath) = $&;
my ($scanfile) = $';
if ($opt_o) {
  $scanfile =~ s/_CALIB_AMP2.TXT$//;
}else {
  $scanfile =~ s/_[^_]*?_CALIB_AMP2.TXT$//;
}
$scanfile .= '.SCAN';
$scanfile = $scanpath . $scanfile;
print "scanfile = $scanfile\n";

my (@freqsum) = ();
if (-e $scanfile) {
  open (SCANFILE, "$scanfile") || die "can\'t open file $scanfile: $!\n";
  my ($freqstart) = 0;
  SCAN: while (<SCANFILE>) {
    chomp;
    #s/^\s+//;
    if (/Frequency\s+Table/i) { $freqstart = 1; next SCAN}

    my ($line) = $_;
    if ($freqstart) {
      push @freqsum, $line;
    }
  }
}else {
  print "SCAN file not found\n"
}

  

#read in the data
my ($scale) = 0;
my (%amp) = ();
my (%amperr) = ();
my ($ifname);
my ($details);
READFILE: while (<INFILE>) {
  chomp;
  s/^\s+//;
  if (/File =/) {
    $details = $_;
    #print "$details\n";
    next READFILE ;
  }

  if (/Gain amplitudes/) {
    my ($line) = $_;
    $line =~ s/.*Gain\s+amplitudes,//;
    my (@data) = split (/=/, $line);
    $scale = $data[1]/$data[0];
    #print "scale=$scale\n";
    #<STDIN>;
    next READFILE;
  }

  if (/Stokes\s+=\s+(\w)\s+IF\s+=\s+(\w+)\s+Freq\s+=\s+([\d\.]+)/) {
    my ($stokes) = $1;
    my ($ifnum) = $2;
    my ($freq) = $3;
    $ifname = $stokes . $ifnum;
    #print "ifname = $ifname\n";
    #<STDIN>;
  }

  if (/^\d{2}:\d{2}:\d{2}/) {
    my ($line) = $_;
    my ($endofline) = length ($line);
    my ($tel) = 0;
    my ($length) = 5;
##aips 31DEC13 added an extra [.s] to the time format
###automatic detector of whether there is an extra [.s] in setting offset
    my ($offset) = 18;
    if ($line =~ /^\d{2}:\d{2}:\d{2}\.\d/) {
      $offset = 20;
    }
    while ($offset < $endofline) {
      my ($data) = substr ($_, $offset, $length);
      if ($data =~ /\d/) {
        $data = $data * $scale;
        push @{$amp{$ifname}[$tel]}, $data;
        push @{$amperr{$ifname}[$tel]}, abs($data-1);
      }
      $offset += 6;
      ++$tel;
    }
  }

}
close (INFILE);


my (%stats);
foreach my $ifname (sort keys %amp) {
  #print "\n" x 5, "$ifname\n";
  for (my $tel=0; $tel < @{$amp{$ifname}}; ++$tel) {
    my ($telaips) = $tel+1;
    #print "\nstation=$telaips\n";
    if (defined $amp{$ifname}[$tel]) { 
      #print "@{$amp{$ifname}[$tel]}\n";
      my ($ndata, $sum, $stdev, $stdevpop, $mean, $median, $stderr, $xmax,
                $xmin) = stats( @{$amp{$ifname}[$tel]} );

      #print 'The number of data points is: ', $$ndata, "\n";;
      #print "The sum of data points is: $$sum\n" ;
      #print "The stdev in the sample (sig(n)) is: $$stdev\n";
      #print "The stdev in the population (sig(n-1)) is: $$stdevpop\n";
      #print "The mean is: $$mean\n";
      #print "The median is: $$median\n";
      #print "The standard error on the mean is: $$stderr\n";
      #print "The maximum of the data is: $$xmax\n";
      #print "The minimum of the data is: $$xmin\n";
      $stats{$tel}{$ifname}{'ndata'} = $$ndata;
      $stats{$tel}{$ifname}{'mean'} = sprintf ("%.4f", $$mean);
      $stats{$tel}{$ifname}{'median'} = sprintf ("%.4f", $$median);
      $stats{$tel}{$ifname}{'stderr'} = sprintf ("%.5f", $$stderr);
      $stats{$tel}{$ifname}{'max'} = $$xmax;
      $stats{$tel}{$ifname}{'min'} = $$xmin;

      my ($mederr) = (stats( @{$amperr{$ifname}[$tel]} ))[5];
      $stats{$tel}{$ifname}{'mederr'} = sprintf ("%.4f", $$mederr);
    }

  }
}

open (OUTFILE, ">$outfile");

print OUTFILE "$details\n";
foreach my $freq (@freqsum) {
  print OUTFILE "$freq\n";
}
foreach my $tel (sort {$a <=> $b} keys %stats) {
  my (@telmedian) = ();
  my (@telmederr) = ();
  my ($telaips) = $tel+1;
  print OUTFILE "\nstation=$telaips";
  print OUTFILE "($antnum{$telaips})" if ($antnum{$telaips});
  print OUTFILE "\n";
  print OUTFILE "IF  median  med.err  stderr  med.err-(med-1)  ndata\n";
  foreach my $ifname (sort keys %{$stats{$tel}}) {
    my ($meddiff) = $stats{$tel}{$ifname}{'mederr'} -
                    abs ($stats{$tel}{$ifname}{'median'} - 1);
    $meddiff = sprintf ("%.4f", $meddiff);
    print OUTFILE "$ifname  $stats{$tel}{$ifname}{'median'}  ",
        "$stats{$tel}{$ifname}{'mederr'}  $stats{$tel}{$ifname}{'stderr'}  ",
        "$meddiff            $stats{$tel}{$ifname}{'ndata'}\n";
    push @telmedian, $stats{$tel}{$ifname}{'median'};
    push @telmederr, $stats{$tel}{$ifname}{'mederr'};
  }
  if (@telmedian > 1) {
    my ($telmedian, $telerr) = (stats(@telmedian))[5 .. 6];
    my ($telmederr) = (stats(@telmederr))[5];
    my ($telmeddiff) = $$telmederr  - abs ($$telmedian -1);
    $$telerr = sprintf ("%.5f", $$telerr);
    $$telmederr = sprintf ("%.5f", $$telmederr);
    $$telmedian = sprintf ("%.4f", $$telmedian);
    $telmeddiff = sprintf ("%.4f", $telmeddiff);
    print OUTFILE "All:  $$telmedian  $$telmederr  $$telerr  $telmeddiff         ",
                scalar (@telmedian), "\n";
  }
}



##########################
sub usage {
  print <<EOF
usage: ampcalstats.pl [options] <filename>
options: -o if old style ampcal file names
EOF
      ;

}
