#!/usr/bin/perl -w

# Description: Perl scriptfile for JIVE to copy datafiles from
#              jaw0 to juw17 for inspection by aips++ utilities. 
#              small changes different data locations, Huib 130799
# 21/07/99  Major changes by CJP. Now recursivly copies all subdirectories
# 14/09/99  If project option is not supplied, show list of projects
# 17/11/99  Read scans from (optional) external list
# 20/04/00  Major logic rewrite to transparently read from multiple disks
# 21/06/02  make default source of data juw16 rather than jaw0
# 04/07/02  Cleaned up change to juw16 and force ftp of toplevel vex from jaw0
# 12/09/02  also get logbook by default (rmc)
# 27/09/02  Accept also possibility of *.lis format SJ specification
# 27/10/03  Change access of juw16 -> juw26 (BK)
# 10/02/04  Add juw26_7,_8 to list of disks on juw26.
# 11/14/06  change CCC from jaw0 to juw31/32==ccsops (& associated data dir)
# 08/08/08  change DDD from juw26(a) to juw36 (& associated data dir)
# 08/08/08  change "-jaw0" switch to "-juw26" for getting data off juw26
# 03/08/11  add DDD directory structure for data correlated on sfxc
#   idem    remove -juw26 switch
# 08/11/11  make sfxc default DDD-path; add -mk4 switch for MkIV DDD-path
# 30/01/15  shift CCC to ccsbeta.nfra.nl; DDD to out.sfxc.nfra.nl
# 30/01/15  convert all invocations of FTP to SSH/SCP/etc.

use Getopt::Long;

use File::Basename;
use Cwd;
use File::Copy;

#use constant CCC => 'jaw0.nfra.nl';
#use constant CCC => '10.88.0.35';
#use constant CCC => 'ccsops.nfra.nl';
use constant CCC => 'ccs';

#use constant DDD => 'juw26.nfra.nl';
#use constant DDDOLD => 'juw26a';
#use constant DDD => 'juw36.nfra.nl';
use constant DDD => 'out.sfxc.nfra.nl';

#use strict;

sub unique (\@);
sub getdir_fast ($$$);
sub getval ($$);

# These default options may be changed
#my $host1        = DDDOLD;
my $host1        = DDD;
my $CCChost      = CCC;
my $host = undef;
my $user        = 'jops';

my @defaultDirs1 = ('/data/sfxc');
my @defaultDirs2 = ('/data');

# Don't change these!
my @baseDirs    = ();
my $jobList     = undef;
my $lisFile     = undef;
my $projectName = undef;
my $overwrite   = 0;
my $all         = 0;
my $ddd         = 1;
my $fixvex      = 1; # Fix up the output vexfiles (break too-long comment)
my $getvex      = 1; # Do check the top level vexfile is there
my $singlemode  = 0; # Change mode name to SingleMode (not needed for modern releases)
my $nologbk     = 0; # get logbook every time run (ensures being updated) rmc
my $debug       = 0;
my $mk4         = 0; # data correlated on MkIV; different DDD directory structure

# if basename(CWD) looks like an experiment ID... set
# the (default) project name to this
my $defproj = undef;

my $tmpproj = basename(getcwd);
if( $tmpproj =~ m/^[A-Z][A-Z0-9]{4,5}/i ) {
	$defproj = $tmpproj;
	$defproj =~ tr/a-z/A-Z/;
}


########################################
# Parse the command line
########################################

my $help = 0;
GetOptions('project=s'=>\$projectName, 'user=s'=>\$user, 'host=s'=>\$host,
	   'debug'=>\$debug, 'help'=>\$help, 'all'=>\$all, 'scans=s'=>\$jobList,
	   'overwrite'=>\$overwrite, 'basedir=s'=>\@baseDirs, 'ddd!'=>\$ddd,
	   'fix!'=>\$fixvex, 'vex!'=>\$getvex, 'singlemode'=>\$singlemode, 
           'mk4'=>\$mk4, 'nolog!'=>\$nologbk, 'lis=s'=>\$lisFile);
#          'juw26'=>\$juw26, 'nolog!'=>\$nologbk, 'lis=s'=>\$lisFile);

die "Can't use -lis and -scans together\n" if (defined $lisFile && defined $jobList);
if (!defined $host) {
    $host = $host1;
}
if (defined $lisFile) {  #If -lis specified, query for top-level vexfile
  $getvex = 0;
}

usage() if $help;
if (!@baseDirs) { # No basedirs supplied
  if ($host eq DDD) {
    @baseDirs = @defaultDirs1;    
    if ($mk4) {   # mk4 data path
      @baseDirs = @defaultDirs2;
      my $fixvex = 0;
    }
  } else {
    die "Must supply basedir along with hostname\n";
  }
}

########################################
# Get the list of projects
########################################

# Get a listing of the projects in the base directories
my @projects = ();
if ($ddd) {
  foreach my $DDDdir (@baseDirs) {
    my @theseProjects = (); # Temporary array to hold projects on this disk

    my $dirList = `ssh -l $user -x $host ls -l $DDDdir`;
    my @dirList = split("\n",$dirList);

    foreach my $dirLine (@dirList) {
      if (substr($dirLine,0,1) eq "d") {  # If name is a regular dir name
	my @dirFields  = split(" ",$dirLine);
	my $projName = $dirFields[8];
	next if ($projName eq '.' || $projName eq '..'); # Needed for Solaris FTP 
	push @theseProjects, $projName;
      }
    }

    push @projects, \@theseProjects; # Save this list of projects
  }

  # Create a single list of projects
  my @projectList = ();
  foreach (@projects) {
    push @projectList, @$_;
  }
  die "No projects found in @baseDirs\n" if (scalar(@projectList)==0);

  # Remove duplicates
  unique(@projectList);

  if (!defined $projectName) {
    print "Projects:\n";
    foreach (@projectList) {
      print "   $_\n";
    }
   
    if( (defined $defproj) &&
        !grep(/^$defproj$/, @projectList) ) {
	print "There seems to be no more data for this ";
	print "project [$defproj] on disk??\n";
    	my $ans = getval("*** Do you still want to continue?", 'N');
        $ans =~ s/\s//g; # Remove spaces
	'yes' =~ /^$ans/i || die "\nOkiedokie then :)";
	undef $defproj;
	
    }
    if( !defined $defproj ) {
        $defproj = $projectList[0];
    }
    $projectName = getval("\nEnter project name", $defproj);
  }

  # Check the directory exists
  if (! grep(/^$projectName$/, @projectList)) {
    die "$projectName is not a experiment name!\n";
  }
} else {
  $projectName = '';
}

########################################
# Check if there is an toplevel vexfile
########################################
if ($getvex) {
  my $cwd = basename(cwd);
  if (! -e "${cwd}.vix") { # Top level vex file does not exist yet
    my $ans = getval("${cwd}.vix does not exist, do you want to get it?", 'N');
    $ans =~ s/\s//g; # Remove spaces
    if ('yes' =~ /^$ans/i) {
      # OK list the contents of the experiment directory

	my $exprDir = '/ccs/expr/'. uc($projectName);
      
        my $dirList = `ssh -l $user -x $CCChost ls -l $exprDir`;
        my @dirList = split("\n",$dirList);

	my @vexFiles;
	my $bestvex = undef;
	foreach (@dirList) { # Sort and exclude a few files
	  if (substr($_,0,1) ne "d") {  # Exclude directories
	    my @fields  = split;
	    my $fileName = $fields[8];
	    if (defined $fileName) {
	      next if $fileName =~ /\~$/;
	      next if $fileName =~ /\.sh$/;
	      next if $fileName =~ /^runfile/;
	      next if $fileName =~ /\.def$/;
	      next if $fileName =~ /^\./;
	      next if $fileName =~ /^\#/;

	      push @vexFiles, $fileName;
	      if (!defined $bestvex && $fileName =~ /\.vix$/) {
		$bestvex = $fileName;
	      }
	    }
	  }
	}
	$bestvex = $vexFiles[0] if (!defined $bestvex);

	print "Requested vexfile?\n";
	foreach (@vexFiles) {
	  print "  $_\n";
	}
	my $vex = '';
	while ($vex eq '') {
	  my $ans = getval("Vexfile to copy (n for none): ", $bestvex);
	  $ans =~ s/\s//g;
	  if ('None' =~ /^$ans/i) {
	    $vex = 'None';
	  } else {
	    foreach (@vexFiles) {
	      if ($ans eq $_) {
		$vex = $_;
		last;
	      }
	    }
	    print "$ans does not exist\n" if ($vex eq '');
	  }
	}
	if ($vex ne 'None') {
          system("scp jops\@$CCChost\:$exprDir/$vex .");

## $singlemode obsolete -- could be deleted
	  if ($singlemode) {
	    # Now try and change the mode name to 'SingleMode';
	    open(VEXFILE, $vex) || die "Could not open $vex: $!\n";
	    my $gotsched = 0;
	    while (<VEXFILE>) {
	      if (/^\s*\$SCHED\s*\;/) { # Schedule block
		$gotsched = 1;
		last;
	      }
	    }
	    die "No \$SCHED section found in $vex\n" if (! $gotsched);
	    my %modes = ();
	    while (<VEXFILE>) {
	      last if /^\s*\$\S+\s*\;/; # Next block
	      if (/^[^\*]*mode\s*=\s*(\S+)\s*\;/) { # Schedule block
		$modes{$1} = 1;
	      }
	    }
	    close(VEXFILE);
	    my @modes = keys(%modes);
	    if (@modes==1) { # Just one mode found
	      my $mode = $modes[0];
	      print "Changing $mode to 'SingleMode'\n";
	    
	      open(VEXFILE, $vex) || die "Could not open $vex: $!\n";
	      open(NEWVEX, ">${cwd}.vix") || die "Could not open ${cwd}.vix: $!\n";
	    
	      # Skip to the MODE section
	      my $gotmode = 0;
	      while (<VEXFILE>) {
		print NEWVEX;
		if (/^\s*\$MODE\s*\;/) {
		  $gotmode = 1;
		  last;
		}
	      }
	      die "No \$MODE section found in $vex\n" if (!$gotmode);
	      $gotmode = 0;
	      while (<VEXFILE>) {
		if (/^\s*def\s*$mode\s*\;/) {
		  print NEWVEX "*$_";
		  print NEWVEX "def SingleMode;\n";
		  $gotmode = 1;
		  last;
		} else {
		  print NEWVEX;
		}
	      }
	      die "No $mode mode definition found\n" if (!$gotmode);
	    
	      # Copy the rest
	      while (<VEXFILE>) {
		print NEWVEX;
	      }
	      close(VEXFILE);
	      close(NEWVEX);
	    } else {
	      print "Multiple modes (@modes) being used.\nCannot force to 'SingleMode'\n";
	    }
	  } else {
##          # if .not.singlemode (not to be deleted)
	    # Just copy the vexfile to a new name (if needed)
		if ($vex ne "${cwd}.vix") {
    	    copy($vex,"${cwd}.vix");
		}
	  }#if_singlemode
	}
    }  
  }
}

########################################
# Get VEXfile specified in *.lis file, if using -lis input  (rmc 27sep2002)
########################################
if (defined $lisFile) {
  # Check the *.lis file exists
  die "$lisFile does not exist\n" if (! -e $lisFile);
  die "$lisFile is not a plain file\n" if (! -f _);
  die "$lisFile is not readable\n" if (! -r _);

  # Extract VEX file from line with 5 strings (not constrained to be 1st)
  open(LISFILE, $lisFile) || die "Cannot open $jobList: $!\n";
  my $vxfl = undef;
  # loop over whole file, skipping lines once $vxfl set
  while (<LISFILE>) {
    next if (defined $vxfl);
    s/\#.*//;            # remove comments from line
    next if (/^\s*$/);   # skip if blank
    if (/^\s*(\S+)\s*(\S+)\s*(\S+)\s*(\S+)\s*(\S+)\s*$/) {  
      $vxfl = $2;
    }
  }
  close(LISFILE) || die "Error closing $lisFile: $!\n";
  die "No VEX-file found in $lisFile: $!\n" if (!defined $vxfl);

    my $vexDir = '/ccs/expr/'.uc($projectName);
    system("scp jops\@$CCChost\:$vexDir/$vxfl .");

  #prep for using $lisFile as SJ list w/o having to change code in
  #   "Read the external list of jobs" section
  #  Note: -lis & -scans shouldn't be used at same time
  $jobList = $lisFile;   

} #IF defined $lisFile

########################################
# Get list of job IDs
########################################

my @jobs = (); # List of List of job ids (per base dir)
my @jobList = (); # List of all job ids
for (my $i=0; $i<@baseDirs; $i++) {
  $jobs[$i] = [];
  # Does this base directory contain this project
  next if ($ddd && ! grep(/^$projectName$/, @{$projects[$i]}));

  my $prjDir ="$baseDirs[$i]/$projectName";
  # Move to this directory
  
  my $dirList = `ssh -l $user -x $host ls -l $prjDir`;
  my @dirList = split("\n",$dirList);

  # Grab all the job ids from thie base directory
  foreach my $dirLine (@dirList) {
    if (substr($dirLine,0,1) eq "d") {  # If name is a regular dir name
      my @dirFields  = split(" ",$dirLine);
      my $dirName = $dirFields[8];
      if ($dirName =~ /^\d+$/) {
	push @{$jobs[$i]}, $dirName;
	push @jobList, $dirName;
      }
    }
  }
}

if (@jobList==0) {
  if ($ddd) {
    die "No jobs found for $projectName\n" 
  } else {
    die "No jobs found in @baseDirs\n";
  }
}

warn "\n**** Warning: Duplicate job ids\n\n" if (unique(@jobList));
@jobList = sort {$a <=> $b} @jobList;

my $first = 1;
########################################
# Read the external list of jobs
########################################

if (defined $jobList) {
  @jobList = ();
  $all = 1;     # Trick to get all the jobs
  $first = 0;

  # Check the file exists
  die "$jobList does not exist\n" if (! -e $jobList);
  die "$jobList is not a plain file\n" if (! -f _);
  die "$jobList is not readable\n" if (! -r _);

  my %uniq = (); # Hash of jobs already collected
  open(JOBLIST, $jobList) || die "Cannot open $jobList: $!\n";
  while (<JOBLIST>) {
    # Remove comments
    s/\#.*//;
    next if (/^\s*$/); # Blank line
# Modify to find either getdata.pl output (Job/SJ ...) or *.lis (+ Job/SJ ...)
#  for *.lis, only get things marked with a +
#  This is different from before, where either (Job ) or (Job/SJ ) worked
#### sfxc job/sj parsing should be the same, even if SJ now means something
####   slightly different

#    if (/^\s*(\S+)\s*$/) {
    if (/^\s*\+?\s*(\d+)\/(\S+)/) {
      my $job = $1;
##      if ($job =~ /(\S+)\/\d+/) { # Ignore possible subjobs
##        $job = $1;
##      }
      if (!defined $uniq{$job}) {
        push @jobList, $job;
        $uniq{$job} = 1;
      }
    } else {
      warn "Ignoring $_";
    }
  }
  close(JOBLIST) || die "Error closing $jobList: $!\n";
  die "No job IDs read!\n" if (scalar(@jobList)==0);
} else {
  print "Job IDs:\n\n";
  foreach (@jobList) {
    print "  $_\n";
  }
}

########################################
# Get the data from the jobs
########################################

my $jobid = $jobList[$#jobList];
while (1) {

  if ($first || !$all) {
    $jobid = getval("\nEnter source name", $jobid);

    if (! grep(/^$jobid$/, @jobList)) {
      print "$jobid is not a job ID!\n";
      next;
    }
  } else {
    $jobid = shift @jobList;
    last if (! defined($jobid)); # No more jobs to get
  }

  # What is the base directory of this job
  my $baseDir = undef;
  for (my $i=0; $i<@baseDirs; $i++) {
    if (grep(/^$jobid$/, @{$jobs[$i]})) {
      $baseDir = $baseDirs[$i];
      last;
    }
  }
  die "Can't find $jobid in any basedir on disk" if (! defined($baseDir));

  getdir_fast("$baseDir/$projectName", $jobid, 0);

  if ($first || !$all) {
    $first=0;
    # Find the index of the current scan
    my $i;
    for ($i=0; $i<@jobList; $i++) {
      last if ($jobList[$i] eq $jobid);
    }

    if ($all) {
      # Remove the leading jobids
      splice @jobList, 0, $i+1;
    } else {
      $jobid = $jobList[$i+1] if ($i+1<@jobList);
    }
  }
}


########################################
# Local sub routines
########################################

sub getdir_fast ($$$) {
  my ($cdir, $subdir, $silent) = @_;

  return if ($subdir eq '.' || $subdir eq '..');
  # Does the directory exist locally?
  if (-e $subdir) {
    if (!$overwrite) {
      warn "Skipping $subdir\n";
      return;
    } else {
      die "$subdir already exists! Cannot (yet) delete it";
    }
  }

  my $cmd;
  system("scp -r jops\@$host\:$cdir/$subdir .");

  # find *.vex file(s) in $subdir and "fixvex'm"
  # (honour the 'fixvex' flag)
  if( -d $subdir && $fixvex )
  {
    # check all *.vex files in this dir
    foreach my $vf (<$subdir/*.vex>) 
    {
      print "Attempting to fix '$vf'...";
	    if( open(VEXFILE, $vf) )
      {
  	    open(NEWVEX, ">${vf}.new") || die "Failed to open ${vf}.new\n";
  	    while (<VEXFILE>)
        {
  	      if( /OLD_prep_job/ )
          {
        		chomp;
            my $printstar=0;
        		while( my $newline = substr($_, 0, 80, '') )
            {
              $printstar++ && print NEWVEX '* ';
        		  print NEWVEX "$newline\n";
		        }
  	      }
          else
          {
        		print NEWVEX;
  	      }
  	    }
	      close(VEXFILE);	    
        close(NEWVEX);

        # Make a backup
        rename $vf, "${vf}.bak" || die "Could not rename $vf: $!\n";
        # Rename new file to orig
        rename "${vf}.new", $vf  
	        || die "Could not rename ${vf}.new to $vf: $!\n";
        print "Ok!\n";
  	  }
      else
      {
  	    warn "Could not open $vf: $!\n";
  	  } # if( open(<sourcevexfile>) )
    } # foreach <sourcevexfile>
  } # if( $subdir exists && $fixvex )
}


# Loop through an array removing duplicate entries (using STRING comparison)
sub unique (\@) {
  my $a = shift;

  my $nsame = 0;
  my $j;
  my $i=1;
  while ($i<@$a) {
    # Is this element the same as any of the previous entries?
    for ($j=0; $j<$i; $j++) {
      if ($a->[$j] eq $a->[$i]) {
	# Remove this element from the list
	splice @$a, $i, 1;
	$nsame++;
	last;
      }
    }
    $i++ if ($j==$i); # ie no duplicates found
  }
  return $nsame;
}

sub getval ($$) {
# Read a value from STDIN with prompt and default
  my($prompt, $default) = @_;
  print "$prompt [$default]: ";
  my $out = <STDIN>;
  chomp($out);
  $out = $default if ($out eq '');

  exit if ('quit' =~ /^\s*$out/i);

  return $out;
}

sub usage {
  print <<EOF
Usage: getdata.pl [options]
  -help                This list
  -project <project>   Project ID. Prompts if not specified
  -all                 Copy all jobs after the first specified one
  -nofix               Do not try to fix the output vexfile
  -novex               Do not try to get the top level vexfile
  -singlemode          Change the toplevel vex \$MODE to SingleMode
  -overwrite           Overwrite existing directories locally
  -scans <file>        Read jobids from file
  -mk4                 For experiments correlated on MkIV 
  -basedir <path>      Use path as the basedir rather than default
  -user <user>         User id for scp session (default jops)
  -host <host>         Host to scp to (default out.sfxc)
  -nolog               Do not try to get the logbook
  -debug               Go through the motions, but don't copy anything

getdata will copy a set of job data from DDD to your local machine. If
required you can specify an alternate host and basedir to get the data
from. Use the -all option to get automatically get all jobs with a
jobid greater than the (interactively) specified id.  The -basedir option 
can be specified multiple times to copy data from more than one disk.

EOF
    ;

  exit(0);
}
