package ParseVex2;
use strict;

## CHANGES
# DAS      changed headstack defn (scalar->array ref)
# ANTENNA  totally changed
# CLOCK    removed "unit"
# SOURCE   added "order" hash entry

# Parse a vex file and return a (complex) hash

BEGIN {
  use Exporter   ();
  use vars       qw($VERSION @ISA @EXPORT @EXPORT_OK @EXPORT_FAIL
		      %EXPORT_TAGS $QUIET );
  $VERSION     = 0.3;
  @ISA         = qw(Exporter);
  @EXPORT      = qw(parse_vex epoch2mjd mjd2epoch VexScans SubJobs);
  %EXPORT_TAGS = ( );
  @EXPORT_OK   = qw( );
  @EXPORT_FAIL = qw( );

  use Carp;
  use Astro::Time qw( dayno2mjd hms2time mjd2dayno time2hms );
}

$QUIET = 0;

##
# Missing
#  ANTENNA -> pointing_sector
#  CLOCK -> only one clock entry per station
#  DAS -> assume tape_length really length
#      -> assume not S2
#######
#  EXPER
#  PROCEDURES
#  SITE -> horizon_map
#  IF
#  BBC
##

# Convert a vex style epoch into an MJD
sub epoch2mjd($) {
  my ($epoch) = @_;

  if ($epoch =~ /^\s*
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

    my $ut = (($second/60+$minute)/60+$hour)/24;

    if ($year!=0) {
       return dayno2mjd($day, $year) + $ut;;
     } else {
       return $day+$ut;
     }
  } else {
    croak "Could not parse $epoch";
  }
}

# Convert an MJD into a vex style epoch
sub mjd2epoch($;$) {
  my $mjd = shift;
  my $short = shift;
  $short = 0 if (!defined $short);

  my ($dayno, $year, $ut) = mjd2dayno($mjd);
  my ($sign, $hour, $minute, $second) = time2hms($ut, 'H', 0);

  my $epoch = sprintf("%03dd%02dh%02dm%02ds",
		      $dayno, $hour, $minute, $second);
  if ($short) {
    return $epoch;
  } else {
    return "${year}y$epoch";
  }
}

# Find the array index of a key in a list (case insensitive)
sub aindex ($$) {
  my ($elem, $array) = @_;
  for (my $i = 0; $i<= $#{$array}; $i++) {
    return $i if ($array->[$i] =~ /^$elem$/i);
  }
  return undef;
}

my @time = qw/psec nsec usec msec sec min hr/; # Treat year seperately
my @time_factor = (1e-12, 1e-9, 1e-6, 1e-3, 1, 60, 60*60);
my @freq = qw/mHz Hz kHz MHz GHz/;
my @freq_factor = (1e-3, 1, 1e3, 1e6, 1e9);
my @length = qw/um mm cm m km in ft/;
my @length_factor = (1e-6, 1e-3, 1e-2, 1, 1e3, 2.54, 2.54*12);
my @angle = qw/mdeg deg amin asec rad/;
my @angle_factor = (1e-3, 1, 1/60, 1/3600, 3.1415926535897932384626433/180);
my @flux = qw/mJy Jy/;
my @flux_factor = (1e-3, 1);
my @sample_rate = qw(ks/sec Ms/sec);
my @sample_rate_factor = (1e3, 1e6);
my @bpi = qw/bpi kbpi/;
my @bpi_factor = (1, 1e3);

# Translate units of $value from $unit to $desired
sub translate_units($$$) {
  my ($value, $unit, $desired) = @_;


  sub matchunit ($$$\@\@) {
    # Return converted factor, or undef if no unit match
    my ($value, $unit, $desired, $units, $factors) = @_;
    my ($index, $factor);

    if (defined($index = aindex($unit, $units))) {
      my $factor = $factors->[$index];
      if (defined($index = aindex($desired, $units))) {
	$factor /= $factors->[$index];
	return $value * $factor;
      } else {
	warn "Unit mismatch: $unit:$desired\n";
	return $value;
      }
    } else {
      return undef;
    }
  }

  my $newvalue;

  $newvalue = matchunit($value, $unit, $desired, @time, @time_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @freq, @freq_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @length, @length_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @angle, @angle_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @flux, @flux_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @sample_rate,
			@sample_rate_factor);
  return $newvalue if (defined $newvalue);

  $newvalue = matchunit($value, $unit, $desired, @bpi, @bpi_factor);
  return $newvalue if (defined $newvalue);

  if ($unit =~ /^yr$/i) {
    warn "Cannot convery yr to $desired\n" if (!($desired =~ /^yr$/i));
    return $value;
  }
  warn "Unknown unit $unit\n";
  return $value;
}


sub translate_compound_units($$$) {
  my ($value, $unit, $desired) = @_;

  my ($unit1, $unit2) = split('/', $unit);
  my ($desired1, $desired2) = split('/', $desired);

  if (!(defined $unit1) || !(defined $unit2)) {
    warn "Cannot translate $unit\n";
    return $value;
  }
  if (!(defined $desired1) || !(defined $desired2)) {
    warn "Cannot translate $desired\n";
    return $value;
  }

  my $factor1 = translate_units(1, $unit1, $desired1);
  my $factor2 = translate_units(1, $unit2, $desired2);

  return $value * $factor1/$factor2;
}


sub parse_vex($;$) {
  my $debug = 0;
  $debug = $_[1] if (defined $_[1]);
  my ($eof);

  my %global = (); # Hash of global info
  # EXPER     scalar (experiment name)
  # EOP       scalar (EOP section)

  my %freq = (); # Hash of frequency definitions
  # Each key points to an array reference containing:
  # rate          scalar
  # nbits         scalar
  # channel def   hash ref (Channel name -> array ref)
  #  freq          scalar
  #  sideband      scalar
  #  bandwidth     scalar
  #  bbc           scalar
  #  polarisation  scalar

  my %mode = (); # Hash of mode definitions
  # Each key points to an array values:
  # procedures   scalar
  # [%freq]       hash ref (Telescope id -> freqmode)
  # [%if]         hash ref (Telescope id -> freqmode)
  # [%bbc]        hash ref (Telescope id -> freqmode)
  # [%tracks]     hash ref (Telescope id -> freqmode)
  # headpos      scalar
  # roll         scalar
  # pass_order   scalar
  # phase_cal    scalar

  my %station = (); # hash of station info (key=station id)
  # One additional hash member with key 'order' is a reference to an array of
  # scalars, representing the order of antenna ids in the vex file.
  # Each key points to an array of values:
  # site         scalar
  # antenna      scalar
  # das          scalar
  # clock        scalar
  # tape         scalar

  my %site = ();    # Hash of site info (key = site name)
  # Each key points to an array of values:
  # site_type      scalar
  # site_name      scalar
  # site_id        scalar
  # site_position  array reference (x,y,z coordinates of site)

  my %antenna = (); # Hash of antenna info (key = antenna name)
  # Each key points to an array of values
  # axis_type        array reference (two values, eg ha/dec or az/el)
  # antenna_diam     scalar [m]
  # antenna_name     scalar
  # axis_orientation scalar
  # axis_offset      scalar [m]
  # antenna_motion   scalar [deg/min]

  my %das = ();     # Hash of das info (key = das type)
  # Each key points to an array of values:
  # record_transport_type  scalar
  # electronics_rack_type  scalar
  # number_drives          scalar
  # headstacks             hash reference
  #  Each element is an array reference, one for each headstack
  #    headstack function   scalar
  #    drive number         scalar
  # record_density         scalar [bpi]
  # tape_length            scalar [ft]
  # tape_motion            array ref
  #    control              scalar
  #    early-start          scalar [min]
  #    late-stop            scalar [min]
  #    min-gap              scalar [sec]
  # tape_control           scalar
  # recording_system_ID    scalar
  # record_transport_name  scalar
  # electronics_rack_ID    scalar
  # electronics_rack_name  scalar

  my %source = ();  # Hash of source info (key = source name)
  # Each key points to an array of values:
  # source_name   scalar
  # ra            scalar
  # dec           scalar
  # epoch         scalar
  # One additional hash member with key 'order' is a reference to an array of
  # scalars, representing the order of sources in the vex file.

  # Track definition
  my %tracks = ();  # Hash of channel to track mappings.
  # Each key points to an array of values:
  # track frame           scalar
  # modulation            scalar (0/1)
  # track                 reference to an array of array references (!)
  #   channel             scalar
  #   sign/mag            scalar (sign/magnitude)
  #   headpass            scalar (Head pass position)
  #   tracks              array ref (track numbers)
  # rate                  scalar  (deprecated?)
  # speed                 scalar  (deprecated?)

  # Head positions
  my %headpos = ();
  # Each key points to an array of scalers
  # The index of the array corresponds to to pass number, the value to head offset

  my @sched = (); # Array of schedule definitions
  # Each element corresponds to a scan in the schedule, and is a reference to an
  # array of:
  # scanname      scalar
  # year          scalar
  # dayno         scalar
  # hour          scalar
  # minute        scalar
  # second        scalar
  # mode          scalar
  # source        scalar
  # stations      hash ref (Station code -> array ref)
  #  late start    scalar
  #  duration      scalar
  #  footage       scalar
  #  pass          scalar
  #  drive         scalar

  my %clock = ();     # Hash of clocks (key = station id)
  # Each key points to an array of values:
  # delay        scalar [usec]
  # rate         scalar [?]
  # epoch        scalar [epoch]
  # valid        scalar [epoch]

  my %eop = ();  # Hash of array references (key = eop block)
  # TAI-UTC         scalar [sec]
  # A1-TAI          scalar [sec]
  # eop_ref_epoch   scalar
  # num_eop_points  scalar
  # eop_interval    scalar
  # ut1-utc         array reference
  # x_wobble        array reference
  # y_wobble        array reference

  my %exper = (); # hash of exper details (key = exper block)
  # Each key points to an array reference
  # exper_num            scalar
  # exper_name           scalar
  # exper_description    scalar
  # PI_name              scalar
  # PI_email             scalar
  # target_correlator    scalar
  # exper_nominal_start  scalar
  # exper_nominal_stop   scalar
  # contact_name         scalar
  # contact_email        scalar
  # scheduler_name       scalar
  # scheduler_email      scalar

  my %tapelog = (); # hash of tapes details (key = station)
  # Each key points to an array reference or array references
  # tape number          scalar
  # VSN                  scalar
  # start time           scalar epoch
  # end time             scalar epoch

  open(VEXFILE, $_[0]) || croak "Failed to open $_[0]: $!\n";

  while (<VEXFILE>) {

    if (/^\s*\$GLOBAL;/) { # The global definition
      warn "\nParsing \$GLOBAL\n\n" if ($debug);

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*ref\s+\$EXPER\s*=\s*(\S+)\s*\;/) {
	  $global{EXPER} = $1;
	  warn "ref \$EXPER = $1\n" if $debug;
	} elsif (/^\s*ref\s+\$EOP\s*=\s*(\S+)\s*\;/) {
	  $global{EOP} = $1;
	  warn "ref \$EOP = $1\n" if $debug;
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$FREQ;/) { # The frequency definition
      warn "\nParsing \$FREQ\n\n" if ($debug);

      my $freqmode = undef;
      my @mode_values = ();
      my $rate = undef;
      my $bits = undef;
      my %channels = ();
      
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	  
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $freqmode = $1;
	  warn "def $freqmode\n" if $debug;
	  
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  
	  if (!defined($freqmode)) {
	    warn "Failed to read \$FREQ section properly\n";
	  }
	  
	  $freq{$freqmode} = [$rate, $bits, {%channels}];
	  
	  $freqmode = undef;
	  $rate = undef;
	  $bits = undef;
	  %channels = ();
	} elsif (/^\s*sample_rate\s*=\s*
		       (\S+)\sMs\/sec\s*\;(\s+           # Sample rate
		       \*\s+\((\d)bits\/sample\))?    # bits - sample
		       /x) {
	  $rate = $1;
	  if (defined $2) {
	    $bits = $2;
	  } else {
	    $bits = '?';
	  }
	  warn "  sample_rate= $rate;  * ($bits bits/sample)\n" if ($debug);
	} elsif (/^\s*chan_def\s*=\s*:\s*
		(\S+)\s*MHz\s*:    # Freq
		\s*([LU])\s*:\s*   # Upper-Lower sideband
		(\S+)\s*MHz\s*:\s* # Bandwidth
		&(\S+)\s*:\s*      # Logical channel
		&(\S+)\s*:\s*      # BBC
		&\S*\s*;           # ?
		 /x) {
	  $channels{$4} = [$1, $2, $3, $5];
	  warn "  chan_def = :  $1 MHz : $2 : $3 MHz : &$4 : &$5;\n" if ($debug);
	}
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$MODE;/) { # The mode definition
      warn "\nParsing \$MODE\n\n" if $debug;
      my ($telescopes, $freqmode, $ifmode, $bbcmode, $trackmode);
      
      my $mode = undef;
      my $procedures = undef;
      my %freq = ();
      my %if = ();
      my %bbc = ();
      my %tracks = ();
      my $headpos = undef;
      my $roll = undef;
      my $pass_order = undef;
      my $phase_cal = undef;
      my @mode_values = ();

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $mode = $1;
	  warn "def $mode\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($mode)) {
	    warn "Failed to read \$MODE section properly\n";
	  } else {
	    push @mode_values, $procedures;
	    push @mode_values, {%freq};
	    push @mode_values, {%if};
	    push @mode_values, {%bbc};
	    push @mode_values, {%tracks};
	    push @mode_values, $headpos;
	    push @mode_values, $roll;
	    push @mode_values, $pass_order;
	    push @mode_values, $phase_cal;   
	  
	    $mode{$mode} = [@mode_values];
	  }
	  
	  $mode = undef;
	  $procedures = undef;
	  %freq = ();
	  %if = ();
	  %bbc = ();
	  %tracks = ();
	  $headpos = undef;
	  $roll = undef;
	  $pass_order = undef;
	  $phase_cal = undef;
	
	  @mode_values = ();
	
	} elsif (/^\s*ref\s+\$PROCEDURES\s+=\s+(\S+)\s*\;/) {
	  $procedures = $1;
	  warn "  \$PROCEDURES= $procedures\n" if $debug;
	} elsif (/^\s*ref\s+\$FREQ\s*=\s*([^:]+)(?:\s*:([^\;]+)\s*)?\;/x) {
	  $freqmode = $1;
	  $telescopes = $2;
	  warn "  \$FREQ= $freqmode $telescopes\n" if $debug;
	  if (defined $telescopes) {
	    foreach (split /:/, $telescopes) {
	      $freq{$_} = $freqmode;
	    }
	  } else {
	    $freq{default} = $freqmode;
	  } 
        } elsif (/^\s*ref\s+\$IF\s*=\s*([^:]+)\s*:([^\;]+)\s*\;/) {
	  $ifmode = $1;
	  $telescopes = $2;
	  warn "  \$IF= $ifmode $telescopes\n" if $debug;
	  foreach (split /:/, $telescopes) {
	    $if{$_} = $ifmode;
	  }
	} elsif (/^\s*ref\s+\$BBC\s*=\s*([^:]+)\s*:([^\;]+)\s*\;/) {
	  $bbcmode = $1;
	  $telescopes = $2;
	  warn "  \$BBC= $bbcmode $telescopes\n" if $debug;
	  foreach (split /:/, $telescopes) {
	    $bbc{$_} = $bbcmode;
	  }
	} elsif (/^\s*ref\s+\$TRACKS\s+=\s+([^:]+)\s*:([^\;]+)\s*\;/) {
	  $trackmode = $1;
	  $telescopes = $2;
	  warn  "  \$TRACKS= $trackmode $telescopes\n" if $debug;
	  foreach (split /:/, $telescopes) {
	    $tracks{$_} = $trackmode;
	  }
	} elsif (/^\s*ref\s+\$HEAD_POS\s+=\s+(\S+)\s*\;/) {
	  $headpos = $1;
	  warn "  \$HEAD_POS= $headpos\n" if $debug;
	} elsif (/^\s*ref\s+\$ROLL\s+=\s+(\S+)\s*\;/) {
	  $roll = $1;
	  warn "  \$ROLL= $roll\n" if $debug;
	} elsif (/^\s*ref\s+\$PASS_ORDER\s+=\s+(\S+)\s*\;/) {
	  $pass_order = $1;
	  warn "  \$PASS_ORDER= $pass_order\n" if $debug;
	} elsif (/^\s*ref\s+\$PHASE_CAL_DETECT\s+=\s+(\S+)\s*\;/) {
	  $phase_cal = $1;
	warn "  \$PHASE_CAL_DETECT= $phase_cal\n" if $debug;
	}
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$STATION;/) { # The station definition
      warn "\nParsing \$STATION\n\n" if ($debug);
      
      my $station = undef;
      my $antenna = undef;
      my $site = undef;
      my $das = undef;
      my $clock = undef;
      my $tape = undef;
      $station{order} = [];
      
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $station = $1;
	  warn "def $station\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($station)) {
	    warn "Failed to read \$STATION section properly\n";
	  } else {
	    $station{$station} = [$site, $antenna, $das, $clock, $tape];
	    push @{$station{order}}, $station;
	  }
	  $station = undef;
	  $antenna = undef;
	  $site = undef;
	  $das = undef;
	} elsif (/^\s*ref\s+\$SITE\s*=\s*(\S+)\s*\;/) {
	  $site = $1; 
	  warn "  \$SITE= $site\n" if $debug;
	} elsif (/^\s*ref\s+\$ANTENNA\s*=\s*(\S+)\s*\;/) {
	  $antenna = $1; 
	  warn "  \$ANTENNA= $antenna\n" if $debug;
	} elsif (/^\s*ref\s+\$DAS\s*=\s*(\S+)\s*\;/) {
	  $das = $1; 
	  warn "  \$DAS= $das\n" if $debug;
	} elsif (/^\s*ref\s+\$CLOCK\s*=\s*(\S+)\s*\;/) {
	  $clock = $1;
	  warn "  \$CLOCK= $clock\n" if $debug;
	} elsif (/^\s*ref\s+\$TAPELOG_OBS\s*=\s*(\S+)\s*\;/) {
	  $tape = $1; 
	  warn "  \$TAPELOG_OBS= $tape\n" if $debug;
	}
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$SITE;/) { # The site definition
      warn "\nParsing \$SITE\n\n" if ($debug);
      
      my $site = undef;
      my $site_type = undef;
      my $site_name = undef;
      my $site_id = undef;
      my $xpos = undef;
      my $ypos = undef;
      my $zpos = undef;
      
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $site = $1;
	  warn "def $site\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($site)) {
	    warn "Failed to read \$SITE section properly\n";
	  } else {
	    $site{$site} = [$site_type, $site_name, $site_id, 
			  [$xpos, $ypos, $zpos]];
	  }
	  $site = undef;
	  $site_type = undef;
	  $site_name = undef;
	  $site_id = undef;
	  $xpos = undef;
	  $ypos = undef;
	  $zpos = undef;
	} elsif (/^\s*\s*site_type\s*=\s*(\S+)\s*\;/) {
	  $site_type = $1; 
	  warn "  site_type= $site_type\n" if $debug;
	} elsif (/^\s*\s*site_name\s*=\s*(\S+)\s*\;/) {
	  $site_name = $1; 
	  warn "  site_name= $site_name\n" if $debug;
	} elsif (/^\s*\s*site_ID\s*=\s*(\S+)\s*\;/) {
	  $site_id = $1;
	  warn "  site_id= $site_id\n" if $debug;
	} elsif (/^\s*\s*site_position\s*=\s*
               (\S+)\s+m\s*:\s*    # x-pos
	       (\S+)\s+m\s*:\s*    # y-pos
	       (\S+)\s+m\s*\;/x) { # z-pos
  	  $xpos = $1;
	  $ypos = $2;
	  $zpos = $3;
	  warn "  site_position= $xpos, $ypos, $zpos\n" if $debug;
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$ANTENNA;/) { # The site/antenna definition
      warn "\nParsing \$ANTENNA\n\n" if $debug;

      my $antenna = undef;
      my $axis1 = undef;
      my $axis2 = undef;
      my $antenna_name = undef;
      my $antenna_diam = undef;
      my $axis_orientation = undef;
      my $axis_offset = undef;
      my %antenna_motion = ();
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $antenna = $1;
	  warn "def $antenna\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($antenna)) {
	    warn "Failed to read \$ANTENNA section properly\n";
	  } else {
	    $antenna{$antenna} = [[$axis1, $axis2], $antenna_diam,
				  $antenna_name, $axis_orientation, 
				  $axis_offset, {%antenna_motion}];
	  }
	  $antenna = undef;
	  $axis1 = undef;
	  $axis2 = undef;
	  $antenna_name = undef;
	  $antenna_diam = undef;
	  $antenna_diam = undef;
	  $axis_orientation = undef;
	  $axis_offset = undef;
	  %antenna_motion = ();
	} else {
	  if (/axis_type\s*=\s*(\S+)\s*:\s*(\S+)\s*\;/) {
	    warn " axis_type = $1 : $2\n" if $debug;
	    $axis1 = $1;
	    $axis2 = $2;
	  }
	  if (/antenna_diam\s*=\s*(\S+)\s*(\S+)\s*\;/) {
	    $antenna_diam = translate_units($1, $2, 'm');
	    warn " antenna_diam = $antenna_diam\n" if $debug;
	  }
	  if (/antenna_name\s*=\s*(\S+)\s*\;/) {
	    $antenna_name = $1;
	    warn " antenna_name = $antenna_name\n" if $debug;
	  }
	  if (/axis_offset\s*=\s*(\S+)\s*(\S+)\s*\;/) {
	    $axis_offset = translate_units($1, $2, 'm');
	    warn " axis_offset = $axis_offset\n" if $debug;
	  }
	  if (/antenna_motion\s*=\s*
	      (\S+)\s*:\s*            # Axis type
	      (\S+)\s+(\S+)\s*:\s*    # Slew rate
	      (\S+)\s+(\S+)\s*\;/x) {    # Settling time
	    $antenna_motion{$1} = [translate_compound_units($2, $3, 'deg/min'),
				   translate_units($4, $5, 'sec')];
	    warn " antenna_motion = $1 : $2 : $4\n" if $debug;
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$DAS;/) { # The das definition
      warn "\nParsing \$DAS\n\n" if ($debug);

      my $das = undef;
      my $record_type = undef;
      my $rack_type = undef;
      my $number_drives = undef;
      my %headstack = ();
      my $density = undef;
      my $tape_length = undef;
      my $tape_motion = undef;
      my $early_start = undef;
      my $late_stop = undef;
      my $min_gap = undef;
      my $tape_control = undef;
      my $recording_system_ID = undef;
      my $record_transport_name = undef;
      my $electronics_rack_ID = undef;
      my $electronics_rack_name = undef;

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;

	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $das = $1;
	  warn "def $das\n" if $debug;

	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($das)) {
	    warn "Failed to read \$DAS section properly\n";
	  } else {
	    $das{$das} = [$record_type, $rack_type, $number_drives,
			  {%headstack}, $density, $tape_length,
			  [$tape_motion, $early_start, $late_stop, $min_gap],
			  $tape_control, $recording_system_ID,
			  $record_transport_name, $electronics_rack_ID,
			  $electronics_rack_name];
	  }
	  $das = undef;
	  $record_type = undef;
	  $rack_type = undef;
	  $number_drives = undef;
	  %headstack = ();
	  $density = undef;
	  $tape_length = undef;
	  $tape_motion = undef;
	  $early_start = undef;
	  $late_stop = undef;
	  $min_gap = undef;
	  $tape_control = undef;
	  $recording_system_ID = undef;
	  $record_transport_name = undef;
	  $electronics_rack_ID = undef;
	  $electronics_rack_name = undef;
	} else {
	  if (/record_transport_type\s*=\s*(\S+)\s*\;/) {
	    $record_type = $1;
	    warn "  record_transport_type = $record_type\n" if $debug;
	  }
	  if (/electronics_rack_type\s*=\s*(\S+)\s*\;/) {
	    $rack_type = $1; 
	    warn "  electronics_rack_type = $rack_type\n" if $debug;
	  }
	  if (/number_drives\s*=\s*(\S+)\s*\;/) {
	    $number_drives = $1;
	    warn "  number_drives = $number_drives\n" if $debug;
	  }
	  if (/headstack\s*=
               \s*(\S+)\s*:  # headstack number
               \s*(\S+)\s*:  # headstack function
               \s*(\S+)\s*\; # drive number
              /x) {
	    $headstack{$1} = [$2, $3];
	    warn "  headstack= $1 : $2 : $3\n" if $debug;
	  }
	  if (/record_density\s*=\s*(\S+)\s+(\S+)\s*\;/) {
	    $density = translate_units($1, $2, 'bpi');
	    warn "  record_density = $density bpi\n" if $debug;
	  }
	  if (/tape_length\s*=\s*(\S+)\s+(\S+)\s*\;/) {
	    $tape_length = translate_units($1, $2, 'ft');
	    warn "  tape_length = $tape_length ft\n" if $debug;
	  }
	  if (/tape_motion\s*=\s*(\S+)\s*:      # motion
              \s*(\S+)\s+(\S+)\s*:              # early start
              \s*(?:(\S+)\s+(\S+))?\s*:         # late stop
              \s*(?:(\S+)\s+(\S+))?\s*\;/x) {  # minimum gap
            $tape_motion = $1;
            $early_start = translate_units($2, $3, 'min');
            $late_stop = translate_units($4, $5, 'min') if (defined $4);
            $min_gap = translate_units($6, $7, 'min') if (defined $6);;
	  }
	  if (/tape_control\s*=\s*(\S+)\s*\;/) {
	    $tape_control = $1;
	    warn "  tape_control= $tape_control\n" if $debug;
	  }
	  if (/recording_system_ID\s*=\s*(\S+)\s*\;/) {
	    $recording_system_ID = $1;
	    warn "  recording_system_ID= $recording_system_ID\n" if $debug;
	  }
	  if (/electronics_rack_ID\s*=\s*(\S+)\s*\;/) {
	    $electronics_rack_ID = $1;
	    warn "  electronics_rack_ID= $electronics_rack_ID\n" if $debug;
	  }
	  if (/electronics_rack_name\s*=\s*(\S+)\s*\;/) {
	    $electronics_rack_name = $1;
	    warn "  electronics_rack_name= $electronics_rack_name\n" if $debug;
	  }
	  if (/record_transport_name\s*=\s*(\S+)\s*\;/) {
	    $record_transport_name = $1;
	    warn "  record_transport_name= $record_transport_name\n" if $debug;
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$SOURCE;/) { # The source definition
      warn "\nParsing \$SOURCE\n\n" if ($debug);

      my $source = undef;
      my $source_name = undef;
      my $ra = undef;
      my $dec = undef;
      my $epoch = undef;
      $source{order} = [];

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	  
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $source = $1;
	  warn "def $source\n" if $debug;
	  
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($source)) {
	    warn "Failed to read \$SOURCE section properly\n";
	  } else {
	    $source{$source} = [$source_name, $ra, $dec, $epoch];
	    push @{$source{order}}, $source;
	  }
	  $source = undef;
	  $source_name = undef;
	  $ra = undef;
	  $dec = undef;
	  $epoch = undef;
	} elsif (/^\s*source_name\s*=\s*(\S+)\s*\;/) {
	  $source_name = $1; 
	  warn "  source_name= $source_name\n" if $debug;
	} else {
	  if (/ra\s*=\s*(\S+)\s*\;/x) {
	    $ra = $1;
	    warn "  ra = $ra;\n" if ($debug);
	  } 
	  if (/dec\s*=\s*(\S+)\s*\;/x) {
	    $dec = $1;
	    warn "  dec = $dec;\n" if ($debug);
	  }
	  if (/ref_coord_frame\s*=\s*(\S+)\s*\;/x) {
	    $epoch = $1;
	    warn "  ref_coord_frame = $epoch;\n" if ($debug);
	  }
	}
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$TRACKS;/) { # The track assignment
      warn "\nParsing \$TRACKS\n\n" if ($debug);
      
      my $trackmode = undef;
      my $modulation = undef;
      my $trackframe = undef;
      my $rate = undef;
      my $speed = undef;
      my @track = ();
      my @tmp;
      my ($headpass, $channel, $sm);
      
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	  
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $trackmode = $1;
	warn "def $trackmode\n" if $debug;
	  
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($trackframe) && !$QUIET) {
	    warn "Failed to read \$TRACKS section properly\n";
	  }
	
	  $tracks{$trackmode} = [$trackframe, $modulation, 
				 [@track], $rate, $speed];
	  
	  $trackmode = undef;
	  $modulation = undef;
	  $trackframe = undef;
	  $rate = undef;
	  $speed = undef;
	  @track = ();
	  
	} elsif (/^\s*data_modulation\s*=\s*(\S+);/) {
	  if ($1 eq 'on') {
	    $modulation = 1;
	  } else {
	    $modulation = 0;
	  }
	  warn " data_modulation = $1\n" if $debug;
	} elsif (/^\s*track_frame_format\s*=\s*(\S+);/) {
	  $trackframe = $1;
	  warn " track_frame_format = $1\n" if $debug;
	} elsif (/^\s*fanout_def\s*=\s*
               (\S)\s:\s+            # Headpass
	       &(\S+)\s*:\s*         # Channel
	       (sign|mag)\s*:\s*     # sign - magnitude
	       \d+\s+                # ?
	       ((?::\s*\d+)+)        # Track on tape
	       /x) {

	  $headpass = $1;
	  $channel = $2;
	  $sm = $3;
	  
	  @tmp = split /:/, $4;
	  # Remove the first blank value
	  shift @tmp;
	  push @track, [$channel, $sm, $headpass, [@tmp]];

	  warn " fanout_def = $headpass : \&$channel : $sm $4\n" if $debug;
	} elsif (/^\s*mode\srequires\s+
	       (\S+)Mb\/s\/tr;\s+    # Data rate
	       tape\sspeed\slow\sdens:\s+
	       (\d+)ips              # Tape speed
	       /x) {
	  $rate = $1;
	  $speed = $2;
	}
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$HEAD_POS;/) { # The head positions
      my $headmode = undef;
      my @headoffset = ();
      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $headmode = $1;
	} elsif (/^\s*enddef/) {
	  if (scalar(@headoffset)==0) {
	    warn 'Failed to read $HEAD_POS section - hope this is a disk ',
                'schedule:';
	  }
	
	  $headpos{$headmode} = [@headoffset];
	
	  $headmode = undef;
	  @headoffset = ();
	
	} elsif (/^\s*headstack_pos\s=\s+
	         (\d+)\s+:\s+         # Pass no.
	         (\S+)\sum            # Head offset
	         /x) {
	  $headoffset[$1] = $2;
      }
      }
      if ($eof) {
	last; 
      } else {
	redo;
      }
    } elsif (/^\s*\$SCHED/) { # The schedule definition
      warn "\nParsing \$SCHED\n\n" if $debug;

      my $scanname = undef;
      my $year = undef;
      my $dayno = undef;
      my $hour = undef;
      my $minute = undef;
      my $second = undef;
      my $mode = undef;
      my $source = undef;
      my %stations = ();

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*scan\s+(\S+)\s*\;/) {
	  $scanname = $1;
	  warn "scan $scanname\n" if $debug;
	} elsif (/^\s*endscan/) {
	  warn "endscan\n\n" if $debug;
	  if (!defined($scanname)) {
	    warn "Failed to read \$SCHED section properly\n";
	  } else {
	    
	    push @sched, [$scanname, $year, $dayno, $hour, $minute, $second, $mode,
			  $source, {%stations}];
	    
	    
	    $scanname = undef;
	    $year = undef;
	    $dayno = undef;
	    $hour = undef;
	    $minute = undef;
	    $second = undef;
	    $mode = undef;
	    $source = undef;
	    %stations = ();
	  }
	} elsif (/^\s*station\s*=\s*(\S+)\s*:\s* # Station
	        (\d+)\s*sec\s*\:\s*      # Start late
	        (\d+)\s*sec\s*\:\s*      # Duration
	        ([\d\.]+)\s*(ft|GB)\s*\:\s*       # Footage
	        (\d+\S){0,1}\s*\:          # Pass
                \s*:\s*               # Wrap?
                (\d+)\s*\;               # Drive
	       /x) {
	  $stations{$1} = [$2, $3, $4, $6, $7];
	  warn "   station=$1 $2 sec: $3 sec: $4 ft: $5: $6\n" if $debug;
	} else { # Parse the individual entries separately
	  if (/start\s*=\s*(\d+)y(\d+)d(\d+)h(\d+)m(\d+)s\s*\;/) {
	    # Start Time
	    $year = $1;
	    $dayno = $2;
	    $hour = $3;
	    $minute = $4;
	    $second = $5;
	    warn " start=${year}y${dayno}d${hour}h${minute}m${second}s\n" if $debug;
	  }
	  if (/mode\s*=\s*(\S+)\s*\;/) { # Mode
	    $mode = $1;
	    warn " mode=$mode\n" if $debug;
	  }
	  if (/source\s*=\s*(\S+)\s*\;/) {    # Source
	    $source = $1;
	    warn " source=$source\n" if $debug;
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$CLOCK;/) { # The clock definition
      warn "\nParsing \$CLOCK\n\n" if ($debug);

      my $station = undef;
      my $valid = undef;
      my $delay = undef;
      my $epoch = undef;
      my $rate = undef;

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;

	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $station = $1;
	  warn "def $station\n" if $debug;

	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($station)) {
	    warn "Failed to read \$CLOCK section properly\n";
	  } else {
	    $clock{$station} = [$delay, $rate, $epoch, $valid];
	  }
	  $station = undef;
	  $valid = undef;
	  $delay = undef;
	  $epoch = undef;
	  $rate = undef;
	} elsif (/^\s*clock_early\s*=\s*
                   (\S+)?\s*\:\s*      # valid from
                   (?:(\S+)\s+(\S+))?  # delay
                   (?:\s*\:\s*(\S+)    # epoch
                   \s*\:\s*(\S+))?     # rate
                   \s*\;/x) {
	  $valid = $1;
	  $delay = translate_units($2, $3, 'usec');
	  $epoch = $4;
	  $rate = $5;

	  if ($debug) {
	    my $warn = '  clock_early = ';
	    $warn .= $valid if (defined $valid);
	    $warn .= " : $delay usec : ";
	    $warn .= $epoch if (defined $epoch);
	    $warn .= ' : ';
	    $warn .= $rate if (defined $rate);
	    $warn .= ";\n";
	    warn $warn;
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$EOP\s*;/) { # The EOP definition
      warn "\nParsing \$EOP\n\n" if ($debug);

      my $eop = undef;
      my $TAI_UTC = undef;
      my $A1_TAI = undef;
      my $eop_ref_epoch = undef;
      my $num_eop_points = undef;
      my $eop_interval = undef;
      my @ut1_utc = ();
      my @x_wobble = ();
      my @y_wobble = ();

      $eof = 1;
      while (<VEXFILE>) {
	s/\*.*//;
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $eop = $1;
	  warn "def $eop\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($eop)) {
	    warn "Failed to read \$EOP section properly\n";
	  } else {
	   $eop{$eop} = [$TAI_UTC, $A1_TAI, $eop_ref_epoch, $num_eop_points,
 		         $eop_interval, [@ut1_utc], [@x_wobble], [@y_wobble]];
	  }
	  $eop = undef;
	  $TAI_UTC = undef;
	  $A1_TAI = undef;
	  $eop_ref_epoch = undef;
	  $num_eop_points = undef;
	  $eop_interval = undef;
	  @ut1_utc = ();
	  @x_wobble = ();
	  @y_wobble = ();
	} else {
	  if (/TAI-UTC\s*=\s*(\S+)\s*(\S+)\s*\;/) {
	    $TAI_UTC = translate_units($1, $2, 'sec');
	    warn "  TAI-UTC = $TAI_UTC sec\n" if $debug;
	  }
	  if (/A1-TAI\s*=\s*(\S+)\s*(\S+)\s*\;/) {
	    $A1_TAI = translate_units($1, $2, 'sec');
	    warn "  A1-TAI = $A1_TAI sec\n" if $debug;
	  }
	  if (/eop_ref_epoch\s*=\s*(\S+)\s*\;/) {
	    $eop_ref_epoch = $1;
	    warn "  eop_ref_epoch = $eop_ref_epoch\n" if $debug;
	  }
	  if (/num_eop_points\s*=\s*(\S+)\s*\;/) {
	    $num_eop_points = $1;
	    warn "  num_eop_points = $num_eop_points\n" if $debug;
	  }
	  if (/eop_interval\s*=\s*(\S+)\s*(\S+)\s*\;/) {
	    $eop_interval = translate_units($1, $2, 'hr');
	    warn "  eop_interval = $eop_interval hr\n" if $debug;
	  }
	  if (/ut1-utc\s*=\s*(\S+\s*\S+ # ut1-utc
               (?:\s*:\s*\S+\s*\S+\s*)*  # more ut1-utc
              )\s*\;/x) {
	    my @uts = split ':', $1;
	    my $first = 1;
	    my $firstunit = undef;
	    foreach (@uts) {
	      my ($ut, $unit) = split;
	      if ($first) {
		$first = 0;
		$firstunit = $unit;
	      } elsif (! defined($unit)) { # unit optional after first entry
		$unit = $firstunit;
	      }
	      push @ut1_utc, translate_units($ut, $unit, 'sec');
	    }
	    if ($debug) {
	      warn '  ut1-utc = ', join(':', @ut1_utc)," sec\n" if $debug;
	    }
	  }
	  if (/x_wobble\s*=\s*(\S+\s*\S+ # x_wobble
               (?:\s*:\s*\S+\s*\S+\s*)*  # more x_wobbles
              )\s*\;/x) {
	    my @wobbles = split ':', $1;
	    my $first = 1;
	    my $firstunit = undef;
	    foreach (@wobbles) {
	      my ($wobble, $unit) = split;
	      if ($first) {
		$first = 0;
		$firstunit = $unit;
	      } elsif (! defined($unit)) { # unit optional after first entry
		$unit = $firstunit;
	      }
	      push @x_wobble, translate_units($wobble, $unit, 'asec');
	    }
	    if ($debug) {
	      warn '  x_wobble = ', join(':', @x_wobble)," asec\n" if $debug;
	    }
	  }
	  if (/y_wobble\s*=\s*(\S+\s*\S+ # y_wobble
               (?:\s*:\s*\S+\s*\S+\s*)*  # more y_wobbles
              )\s*\;/x) {
	    my @wobbles = split ':', $1;
	    my $first = 1;
	    my $firstunit = undef;
	    foreach (@wobbles) {
	      my ($wobble, $unit) = split;
	      if ($first) {
		$first = 0;
		$firstunit = $unit;
	      } elsif (! defined($unit)) { # unit optional after first entry
		$unit = $firstunit;
	      }
	      push @y_wobble, translate_units($wobble, $unit, 'asec');
	    }
	    if ($debug) {
	      warn '  y_wobble = ', join(':', @y_wobble)," asec\n" if $debug;
	    }
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$EXPER\s*;/) { # The EOP definition
      warn "\nParsing \$EXPER\n\n" if ($debug);

      my $exper = undef;
      my $exper_num = undef;
      my $exper_name = undef;
      my $exper_description = undef;
      my $exper_nominal_start = undef;
      my $exper_nominal_stop = undef;
      my $PI_name = undef;
      my $PI_email = undef;
      my $contact_name = undef;
      my $contact_email = undef;
      my $scheduler_name = undef;
      my $scheduler_email = undef;
      my $target_correlator = undef;

      while (<VEXFILE>) {
	s/\*.*//;
	if (/^\$\S+;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $exper = $1;
	  warn "def $exper\n" if $debug;
	} elsif (/^\s*enddef/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($exper)) {
	    warn "Failed to read \$EXPER section properly\n";
	  } else {
	    $exper{$exper} = [$exper_num, $exper_name, $exper_description, 
			      $PI_name, $PI_email, $target_correlator,
			      $exper_nominal_start, $exper_nominal_stop,
			      $contact_name, $contact_email, $scheduler_name,
			      $scheduler_email];
	    $exper = undef;
	    $exper_num = undef;
	    $exper_name = undef;
	    $exper_description = undef;
	    $exper_nominal_start = undef;
	    $exper_nominal_stop = undef;
	    $PI_name = undef;
	    $PI_email = undef;
	    $contact_name = undef;
	    $contact_email = undef;
	    $scheduler_name = undef;
	    $scheduler_email = undef;
	    $target_correlator = undef;
	  }
	} else {
	  if (/exper_num\s*=\s*(\S+)\s*\;/) {
	    $exper_num = $1;
	    warn "  exper_num = $exper_num\n" if $debug;
	  }
	  if (/exper_name\s*=\s*(\S+)\s*\;/) {
	    $exper_name = $1;
	    warn "  exper_name = $exper_name\n" if $debug;
	  }
	  if (/exper_description\s*=\s*(?:(\S+)|"([^"]*)")\s*\;/) {
	    $exper_description = defined($1) ? $1: $2;
	    warn "  exper_description = $exper_description\n" if $debug;
	  }
	  if (/exper_nominal_start\s*=\s*(\S+)\s*\;/) {
	    $exper_nominal_start = $1;
	    warn "  exper_nominal_start = $exper_nominal_start\n" if $debug;
	  }
	  if (/exper_nominal_stop\s*=\s*(\S+)\s*\;/) {
	    $exper_nominal_stop = $1;
	    warn "  exper_nominal_stop = $exper_nominal_stop\n" if $debug;
	  }
	  if (/PI_name\s*=\s*(\S+)\s*\;/) {
	    $PI_name = $1;
	    warn "  PI_name = $PI_name\n" if $debug;
	  }
	  if (/PI_email\s*=\s*(\S+)\s*\;/) {
	    $PI_email = $1;
	    warn "  PI_email = $PI_email\n" if $debug;
	  }
	  if (/contact_name\s*=\s*(\S+)\s*\;/) {
	    $contact_name = $1;
	    warn "  contact_name = $contact_name\n" if $debug;
	  }
	  if (/contact_email\s*=\s*(\S+)\s*\;/) {
	    $contact_email = $1;
	    warn "  contact_email = $contact_email\n" if $debug;
	  }
	  if (/scheduler_name\s*=\s*(\S+)\s*\;/) {
	    $scheduler_name = $1;
	    warn "  scheduler_name = $scheduler_name\n" if $debug;
	  }
	  if (/scheduler_email\s*=\s*(\S+)\s*\;/) {
	    $scheduler_email = $1;
	    warn "  scheduler_email = $scheduler_email\n" if $debug;
	  }
	  if (/target_correlator\s*=\s*(\S+)\s*\;/) {
	    $target_correlator = $1;
	    warn " target_correlator  = $target_correlator\n" if $debug;
	  }
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    } elsif (/^\s*\$TAPELOG_OBS\s*;/) { # The station definition
      warn "\nParsing \$TAPELOG_OBS\n\n" if ($debug);

      my $station = undef;
      my @tapes = ();

      $eof = 1;
      while (<VEXFILE>) {
	if (/^\s*\$\S+\s*;/) {
	  $eof = 0;
	  last;
	} elsif (/^\s*def\s+(\S+)\s*\;/) {
	  $station = $1;
	  warn "def $station\n" if $debug;
	} elsif (/^\s*enddef\s*;/) {
	  warn "enddef\n\n" if $debug;
	  if (!defined($station)) {
	    warn "Failed to read \$TAPELOG_OBS section properly\n";
	  } else {
	    $tapelog{$station} = [@tapes];
	  }
	  @tapes = ();
	  $station = undef;
	} elsif (/VSN\s*=\s*(\d+)\s*:\s* # Drive number
                  (\S+)\s*:\s*           # VSN
                  (\S+)\s*:\s*           # start time
                  (\S+)\s*\;/x) {        # end time
          push @tapes, [$1, $2, $3, $4];
	  warn "  VSN= $1 : $2 : $3 : $4\n" if $debug;
	}
      }
      if ($eof) {
	last;
      } else {
	redo;
      }
    }
  }

  close(VEXFILE);

  my %vexfile = ();

  $vexfile{GLOBAL} = {%global};
  $vexfile{FREQ} = {%freq};
  $vexfile{MODE} = {%mode};
  $vexfile{STATION} = {%station};
  $vexfile{SITE} = {%site};
  $vexfile{ANTENNA} = {%antenna};
  $vexfile{TRACKS} = {%tracks};
  $vexfile{HEADPOS} = {%headpos};
  $vexfile{SOURCE} = {%source};
  $vexfile{DAS} = {%das};
  $vexfile{CLOCK} = {%clock};
  $vexfile{EOP} = {%eop};
  $vexfile{EXPER} = {%exper};
  $vexfile{TAPELOG} = {%tapelog};
  $vexfile{SCHED} = [@sched];

  return \%vexfile;
}

# Return an array containing scan information.
# Each array element is a hash reference with the following keys:
#   scan       Name of scan
#   start      Mjd of scan start
#   end        Mjd of scan end
#   mode       Scan mode
#   source     Scan source
#   stations   Reference to array of antenna IDs
#   subjob     Subjob number
#   headpass   Head pass ('**' if tapes not syncronised)
#   headpasses Reference to array of station head pass
#   direction  Tape direction ('*' if tapes not syncronised);
#   footage    Footage of an arbitrary tape
#
# Usage:
#   my $vex = parse_vex($vexfile);
#   my @scans = VexScans($vex);
#   foreach (@scans) {
#     my %s = %$_;
#
#     printf("%6s %10s-%10s %s %s\n", $s{scan}, mjd2epoch($s{start}), 
#	      mjd2epoch($s{end}), $s{mode}, $s{source});
#     print "       ", @{$s{stations}}, "\n";
#   }
sub VexScans ($;$) {
  my ($vex, $ants) = @_;

  my @scans = ();

  my %headpass = ();
  my %subjob_headpass = ();
  my $subjob = 1;
  foreach (@{$vex->{SCHED}}) {
    my ($footage, $end_mjd, $headpass, $direction);
    my @headpass_list = ();

    my ($scanname, $year, $dayno, $hour, $minute, $second, $mode,
      $source, $stations) = @$_;

    #warn "$scanname\n";

    my @station_list = grep(SelectedStation($_,$ants),
			    keys(%$stations));

    next if (@station_list==0); # No stations selected

    # Get the MJD of the start of the scan
    my $start_mjd = dayno2mjd($dayno, $year);
    $start_mjd += hms2time($hour, $minute, $second);

    if (@station_list) {
      # Get the maximum duration
      my $duration = 0;
      foreach (@station_list) {
	if ($stations->{$_}[1] > $duration) {
	  $duration = $stations->{$_}[1];
	}
      }
      $end_mjd = $start_mjd + $duration/(60*60*24);

      $footage = $stations->{$station_list[0]}[2];

      # Headpass of stations in current scan
      %headpass = map (($_, $stations->{$_}[3]), @station_list);

      # Is is a new subjob?
      # Check if any of the head passes have changed.
      foreach (keys(%headpass)) {
	# If we haven't seen this station in this subjob yet, remember
	# its headpass
	if (! defined $subjob_headpass{$_}) {
	  $subjob_headpass{$_} = $headpass{$_};
	} elsif ($headpass{$_} ne $subjob_headpass{$_}) { # New subjob
	  $subjob++;
	  # Remeber the headpasses of this scan
	  %subjob_headpass = %headpass;
	  last;
	}
      }

      # Get the ordered headpass info
      foreach (@station_list) {
	push @headpass_list, $headpass{$_};
      }

      $direction = '*';
      my @passes = values(%headpass);
      $headpass = $passes[0];
      foreach (@passes) {
	if (defined $_ && $_ ne $headpass) { # Not all tapes are synchronized
	  $headpass = '**';
	  last;
	}
      }
    } else { # Dummy Scan
      $end_mjd = $start_mjd;
      $footage = undef;
      $headpass = undef;
      $direction = undef;
    }

    if (defined($headpass) && $headpass ne '**') {
      if ($headpass =~ /^(\d+)/) {
	if ($1 % 2) { # Odd passes are forwards (?)
	  $direction = 'F';
	} else {
	  $direction = 'R';
	}
      } else {
	warn "Unexpected pass number: $headpass\n";
	$direction = '?';
      }
    }

    push @scans, {scan => $scanname,
		  start => $start_mjd,
		  end => $end_mjd,
		  mode => $mode,
		  source => $source,
		  stations => [@station_list],
		  headpasses => [@headpass_list],
		  subjob => $subjob,
		  headpass => $headpass,
		  direction => $direction,
		  footage => $footage,
		 };
  }
  return @scans;
}

# Return a list of subjobs, based on the SCHED section of the vexfile. 
# Optionally a referene to a list of antenna id's can be passed to make the
# subjob definition closer to ProcJob.
# A new subjob is "declared" if any of the following is true
#  - Any of the tapes change head pass (ie change direction)
#
# Returns an array of hash references, each representing a subjob
# with the following keys:
#   firstscan           First scan in subjob
#   lastscan            Last scan in subjob
#   startime            MJD start time of first scan
#   stoptime            MJD stop time of last scan
#   subjob
#   sources             Array reference to list of scan sources
#   telescopes          Array reference to list of scan telescopes
#   pass                Head pass ('**' if tapes not syncronised)
#   direction           Tape direction ('*' if tapes not syncronised)
#   footage             Initial footage of a random telescope in scan
#
# Usage:
#
#   my $vex = parse_vex($vexfile);
#   my @subjobs = SubJobs($vex);
#   foreach my $sub (@subjobs) {
#     print "FirstScan: $sub->{firstscan}\n";
#     print "LastScan: $sub->{lastscan}\n";
#     print "Sources: ', join('', @{$sub->{sources}}, "\n";
#
sub SubJobs ($;$) {
  my ($vex, $ants) = @_;

  my @subjobs = ();

  my @vexscans = VexScans($vex, $ants);

  my ($s, $start, $end, $firstscan, $lastscan, $headpass, $direction);
  my ($footage, @scans, @sources, @stations);

  my $lastjob = -1;
  while (1) {
    $s = shift @vexscans;

    if (!(defined $s) || $s->{subjob} != $lastjob) { # New subjob or last
      push @subjobs, {starttime => $start,
		      stoptime => $end,
		      firstscan => $firstscan,
		      lastscan => $lastscan,
		      scans => [@scans],
		      subjob => $lastjob,
		      sources => [@sources],
		      stations => [@stations],
		      pass => $headpass,
		      direction => $direction,
		      footage => $footage}
	if ($lastjob!=-1);

      last if (! defined $s); # That was the last subjob

      $start = $s->{start};
      $end = $s->{end};
      $firstscan = $s->{scan};
      $lastscan = $s->{scan};
      $headpass = $s->{headpass};
      $direction = $s->{direction};
      $footage = $s->{footage};
      @scans = ($s->{scan});
      @sources = ($s->{source});
      @stations = ($s->{stations});
      $lastjob = $s->{subjob};

    } else { # Another scan in the same subjob
      $end = $s->{end};
      $lastscan = $s->{scan};

      # Are direction and headpass consistent throughout subjob?
      $headpass = '**' if (defined($s->{headpass})
			   && $headpass ne $s->{headpass});
      $direction = '*' if (defined($s->{direction}) &&
			   $direction ne $s->{direction});

      push @scans, $s->{scan};
      push @sources, $s->{source};
      push @stations, $s->{stations};
    }
  }

  return @subjobs;
}

sub SelectedStation ($$) {
  my ($station, $ants) = @_;
  return(1) if (!defined $ants); # True if no statin selection

  foreach (@$ants) {
    return(1) if (uc($_) eq uc($station));
  }
  return 0;
}

END { }

1;
