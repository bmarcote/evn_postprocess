# useful Perl subroutines
# Cormac Reynolds:  Mar 2004 - original program.
#

use strict;
use Getopt::Long;
use File::Find;
#use PGPLOT;

sub stats {
  # get statistics of the input values
  my (@x) = @_;
  my ($i);

  # get the median of the data
  my ($ndata) = scalar (@x);
  my (@xsrt) = sort {$a <=> $b} (@x);
  my ($mid) = scalar(@x);
  $mid = floor($mid/2) ;
  my ($median) ;
  if ($ndata%2 == 1) {
    $median = $xsrt[$mid];
  }else {
    $median = ($xsrt[$mid] + $xsrt[$mid-1])/2;
  }
  # calculate the straightforward mean and range of the data
  my ($sum) = 0;
  my ($xmax) = $x[0];
  my ($xmin) = $x[0];
  for (my $i=0; $i<@x; ++$i) {
    $sum += $x[$i];
    #print "sum=$sum, x=$x[$i] ";
    if ($x[$i] > $xmax) {$xmax = $x[$i]};
    if ($x[$i] < $xmin) {$xmin = $x[$i]};
  }
  my ($mean) = $sum/scalar(@x);
  # and now the  standard dev. (using both N and N-1 to give the standard dev.
  # of the *sample* and *population* respectively)
  my ($variance) = 0;
  for (my $i=0; $i<@x; ++$i) {
    $variance += ($mean - $x[$i])**2 ;
  }
  my ($varsamp) = $variance/(scalar(@x));
  my ($stdev) = sqrt($varsamp);
  my ($varpop, $stdevpop, $stderr) ;

  if (scalar (@x) > 1) {
    $varpop = $variance/(scalar(@x)-1)  ;
    $stdevpop = sqrt($varpop);
    $stderr = $stdevpop/sqrt(scalar(@x))  ;
  }else {
    $varpop = $stdevpop = $stderr =  0;
  }

  # and write out the results

  #print 'The number of data points is: ', scalar(@x), "\n";;
  #print "The sum of data points is: $sum\n" ;
  #print "The stdev in the sample (sig(n)) is: $stdev\n";
  #print "The stdev in the population (sig(n-1)) is: $stdevpop\n";
  #print "The mean is: $mean\n";
  #print "The median is: $median\n";
  #print "The standard error on the mean is: $stderr\n";
  #print "The maximum of the data is: $xmax\n";
  #print "The minimum of the data is: $xmin\n";
  

  return \$ndata, \$sum, \$stdev, \$stdevpop, \$mean, \$median, \$stderr,
                \$xmax, \$xmin; 
}

sub hogs {
  my (@rootdir) = @_;
  # get list of files 
  my @filelist = ();
  find sub {push @filelist, $File::Find::name}, @rootdir;
  
  my (%hogs) = ();
  foreach my $file (@filelist) {
    if (!-l $file) {
      my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,$atime,$mtime,$ctime,$blksize,$blocks) = stat($file);
      
      my ($user) = getpwuid($uid);
      # some files not owned by a real account:
      if (!$user) {
        $user = $uid;
      }
  
      $hogs{$user}{'sum_size'} += $size/(1024.**2);
      push @{$hogs{$user}{'size'}}, $size/(1024.**2);
      push @{$hogs{$user}{'date'}}, $atime - time();
    }
  }
  return %hogs
}

return 1;
