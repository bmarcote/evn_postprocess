#Not a package
#Just some useful routines

use strict;
use Astro::Time;
use Time::gmtime;
use Time::Local;
#use Time;

sub vex2secs(@){
  my @dates = @_;
  my @answer;

  foreach (@dates){
	 if ($_ =~ /^\s*
		  (?:(\d+)\s*y\s*)?  # Year
		  (?:(\d+)\s*d\s*)?  # Dayno
		  (?:(\d+)\s*h\s*)?  # Hour
		  (?:(\d+)\s*m\s*)?  # Minute
		  (?:(\d+)\s*s)?     # Seconds
		  \s*$/x) {
		my $year = defined($1) ? $1 : 0;
		my $day = defined($2) ? $2 : 0;
		my $hour = defined($3) ? $3 : 0;
		my $minute = defined($4) ? $4 : 0;
		my $second = defined($5) ? $5 : 0;
		
		if ($year==0) {
		  return undef;
		}
		(my $mday, my $mon) = dayno2cal($day,$year);
		#print STDOUT "$hour:$minute:$second - $mday,$mon,$year\n";
		push @answer, timegm($second,$minute,$hour,$mday,$mon-1,$year-1900);
	 }
	 else{
		return undef;
	 }
  }
  #print STDOUT "@answer\n";
  return @answer;
}


sub secs2aipstime (@){
  my @insecs = @_;
  my @answer;

  foreach (@insecs) {
	 my $tm = gmtime($_);
	 push @answer, sprintf("%4d/%02d/%02d/%02d:%02d:%02d", $tm->year+1900, 
						$tm->mon+1, $tm->mday, $tm->hour, $tm->min, $tm->sec);
  }
  return @answer;
}





return 1;
