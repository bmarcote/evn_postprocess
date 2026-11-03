# perl plotting subroutines derived from plot.pl
# intplot is the most useful
# Cormac Reynolds:  Aug 2001 - original program.
#

use strict;
use Getopt::Long;
use PGPLOT;

sub doplot {
  my ($xxin,$yyin,$aspin,$linein,$timein,$titlein,$xlabin,$ylabin,$pgdevin) = @_;
  my ($infile,$i,@data,@x,@y,$temp,$xmin,$xmax,$ymin,$ymax,$deltax,$deltay)=();
  my (@xx,@yy,$nfiles,$j,$symb,$file) = ();
  my ($xl,$xh,$yl,$yh,$xlab,$ylab,$title,$pgdev,$ndata,$colour);

  $title = ${$titlein};
  $xlab = ${$xlabin};
  $ylab = ${$ylabin};
  $pgdev = ${$pgdevin};

  #print "ndim2= ${$ndim}\n";
  #print "ndim=$ndim\n";
  #if (${$ndim} == 1) {
  $nfiles = scalar (@{$xxin});
  #print "nfiles = $nfiles\n";
  for ($i=0;$i<@{$xxin}; ++$i) {
    for ($j=0; $j<@{${$xxin}[$i]}; ++$j) {
      $xx[$i]->[$j] = ${$xxin}[$i]->[$j];
      $yy[$i]->[$j] = ${$yyin}[$i]->[$j];
    }
  }


    #foreach $temp (@{$xxin}) {
    #  @xx = $temp;
    #}
    #foreach $temp (@{$yyin}) {
    #  @yy = $temp;
    #}
  #}
  my ($asp) = ${$aspin};
  my (@line) = ();
  my ($data) = ();
  if (defined $linein) {
    foreach $data (@{$linein}) {
      push @line, $data;
    }
  }
  my ($time) = ${$timein}  ;

  $nfiles = scalar (@xx);
  for ($i=0; $i<$nfiles; ++$i) {
    $ndata = scalar(@{$xx[$i]});
    #print "ndata($i) = $ndata\n";
  }
  #print "@x\n";
  #print "@y\n";
  #$temp = scalar(@x);
  #print "xdata=$temp\n";
  #$temp = scalar(@y);
  #print "ydata=$temp\n";

  # do that plotting thang

  # find the max and min values in the arrays
  $xmin = ${$xx[0]}[0] ;   # = 0.0;
  $xmax = ${$xx[0]}[0] ;  # = 10.0;
  $ymin = ${$yy[0]}[0] ;   # = 0.0;
  $ymax = ${$yy[0]}[0] ;  # = 10.0;
  $i=0;
  while ($i<@xx) {
    $j=0;
    while ($j < scalar(@{$xx[$i]})) {
      if (${$xx[$i]}[$j] < $xmin) {$xmin = ${$xx[$i]}[$j]}
      if ($xx[$i]->[$j] > $xmax) {$xmax = $xx[$i]->[$j]}
      if ($yy[$i]->[$j] < $ymin) {$ymin = $yy[$i]->[$j]}
      if ($yy[$i]->[$j] > $ymax) {$ymax = $yy[$i]->[$j]}
      ++$j;
    }
    ++$i;
  }

  # set the plot extrema
  #print "xmax=$xmax, xmin=$xmin, ymax=$ymax, ymin=$ymin \n";
  $deltax = $xmax - $xmin;
  $deltay = $ymax - $ymin;
  # this is a little workaround in case deltax or deltay ~ 0
  if ($deltax < 0.000000001) {
    $deltax = 0.1;
    $xmax=$xmax+$deltax/2;
    $xmin=$xmin-$deltax/2;
  }
  if ($deltay < 0.000000001) {
    $deltay = 0.1;
    $ymax=$ymax+$deltay/2;
    $ymin=$ymin-$deltay/2;
  }
  $xl = $xmin - 0.1*$deltax;
  $xh = $xmax + 0.1*$deltax;
  $yl = $ymin - 0.1*$deltay;
  $yh = $ymax + 0.1*$deltay;
  $colour = 0;
  $symb = 2;
  pgbeg(0, $pgdev, 1, 1) || die "Error opening PGPLOT device\n";
  pgvport(0.2,0.8,0.2,0.8);

  for ($file=0; $file<$nfiles; ++$file) {
    @x = @{$xx[$file]};
    @y = @{$yy[$file]};
    #print "x2=@x\n";
    #print "y2=@y\n";

    #pgadvance();
    pgsci(1);
    #pgenv($xl, $xh, $yl, $yh, 0, 0);
    pgsch(1.0);
    unless ($asp) {
      pgswin($xl, $xh, $yl, $yh);
    }else {
      pgwnad($xl, $xh, $yl, $yh);
    }
    unless ($time) {
      pgtbox ('bctns',0.0,0,'bctns',0.0,0);
    }else {
      pgtbox ('bctnszh',0.0,0,'bctns',0.0,0);
    }

    # plot titles and labels.
    #$xlab = "X";
    #$ylab = "Y";
    #$title = "";
    pglab($xlab, $ylab, $title);
    pgsch(1.3);
    ++$colour;
    ++$symb;
    pgsci($colour);
    $ndata = scalar(@x);
    my ($isline) = 0;
    foreach $data (@line) {
      if ($data == $file+1) {
        $isline = 1;
      }
    }
    if ($isline == 1) {
      pgline($ndata, \@x, \@y);
    }else {
      pgpoint($ndata, \@x, \@y, $symb);
    }
  }
  pgiden();
  pgend();
}

sub intplot {
  my ($xxin,$yyin,$aspin,$linein,$timein,$titlein,$xlabin,$ylabin,$pgdevin,$interact,$limitsin,$textin) = @_;
  # interactive plotting routine. All inputs except $xxin and $yyin are
  # optional. All inputs should be passed as references.
  # $xxin and $yyin are arrays of arrays, each pair of xxin and yyin arrays
  # gets plotted with a different symbol/colour on the same plot.
  #
  # Description of inputs:
  # $xxin: array of arrays of x data
  # $yyin: corresponding array of arrays of y data
  # $aspin: =0 means don't maintain aspect ratio, > 0 means maintain aspect
  # $linein: array listing $xxin arrays which should be plotted as lines
  #     rather than points
  # $timein: >0 means the x data are seconds, and you would like x-axis tics to
  #     be in day, hour, min, sec format
  # $titlein: the plot title
  # $xlabin: label for the x-axis
  # $ylabin: label for the y-axis
  # $pgdevin: the pgplot device, '?' will give options
  # $interact: >0 will initiate interactive plotting
  # $limitsin: array to set the x and y limits if desired (otherwise
  #      self-scales). Order of elements is (xmin, ymin, xmax, ymax).
  # $textin: array of notes to put at the end of each plot (array corresponds to
  #     $xxin

  my ($infile,$i,@data,@x,@y,$temp,$xmin,$xmax,$ymin,$ymax,$deltax,$deltay)=();
  my (@xx,@yy,$nfiles,$j,$symb,$file)=();
  my ($xl,$xh,$yl,$yh,$xlab,$ylab,$title,$pgdev,$ndata,$colour,@limits);
  my (@xbox1, @xbox2, @ybox1, @ybox2) = ();

  # change the inputs from references to normal variables, for sanity
  $title = ${$titlein};
  $xlab = ${$xlabin};
  $ylab = ${$ylabin};
  $pgdev = ${$pgdevin};
  $interact = ${$interact};

  $nfiles = scalar (@{$xxin});
  #print "nfiles = $nfiles\n";
  for ($i=0;$i<@{$xxin}; ++$i) {
    if (defined @{${$xxin}[$i]}) {
      for ($j=0; $j<@{${$xxin}[$i]}; ++$j) {
        $xx[$i]->[$j] = ${$xxin}[$i]->[$j];
        $yy[$i]->[$j] = ${$yyin}[$i]->[$j];
      }
    }
  }

  if (defined $limitsin) {
    for ($i=0; $i<@{$limitsin}; ++$i) {
      if (defined ${$limitsin}[$i]) {
        #print ${$limitsin}[$i];
        $limits[$i] = ${$limitsin}[$i]   
      }
    }
  }


  my ($asp) = ${$aspin};
  my (@line) = ();
  #my ($data) = ();
  if (defined $linein) {
    foreach my $data (@{$linein}) {
      push @line, $data;
    }
  }
  my ($time) = ${$timein}  ;

  my (@text) = ();
  if (defined $textin) {
    foreach my $data (@{$textin}) {
      push @text, $data;
    }
  }

  $nfiles = scalar (@xx);
  for ($i=0; $i<$nfiles; ++$i) {
    $ndata = scalar(@{$xx[$i]})  if defined (@{$xx[$i]});
    #print "ndata($i) = $ndata\n";
  }
  #print "@x\n";
  #print "@y\n";

  # do that plotting thang

  if ($interact) {
    print "Draw boxes around data using mouse-left (A) to select\n";
    print "Press mouse-middle (D) to restart current box\n";
    print "Press mouse-right (X) to exit\n";
    print "Type 'Z' to describe a zoom box\n";
    print "Type 'U' to unzoom\n";
    print "Type 'C' anywhere on plot to cancel previously drawn boxes\n";
  }

  my ($zoom) = ();
  my ($replot) = 1;
  pgbeg(0, $pgdev, 1, 1) || die "Error opening PGPLOT device\n";

  while ($replot) {
    pgeras() if $interact;

    # find the max and min values in the arrays
    #$xmin = ${$xx[0]}[0] ;   # = 0.0;
    #$xmax = ${$xx[0]}[0] ;  # = 10.0;
    #$ymin = ${$yy[0]}[0] ;   # = 0.0;
    #$ymax = ${$yy[0]}[0] ;  # = 10.0;
    $i=0;
    while ($i<@xx) {
      $j=0;
      while ($j < scalar(@{$xx[$i]})) {
        if (defined $xmax) {
          if (${$xx[$i]}[$j] < $xmin) {$xmin = ${$xx[$i]}[$j]}
          if ($xx[$i]->[$j] > $xmax) {$xmax = $xx[$i]->[$j]}
          if ($yy[$i]->[$j] < $ymin) {$ymin = $yy[$i]->[$j]}
          if ($yy[$i]->[$j] > $ymax) {$ymax = $yy[$i]->[$j]}
        }elsif (defined $xx[$i]->[$j]) {
          $xmin = ${$xx[$i]}[$j];
          $xmax = $xx[$i]->[$j];
          $ymin = $yy[$i]->[$j];
          $ymax = $yy[$i]->[$j];
        }
        ++$j;
      }
      ++$i;
    }


    # set the plot extrema
    #print "xmax=$xmax, xmin=$xmin, ymax=$ymax, ymin=$ymin \n";
    $deltax = $xmax - $xmin;
    $deltay = $ymax - $ymin;
    # this is a little workaround in case deltax or deltay ~ 0
    if ($deltax < 0.000000001) {
      $deltax = 0.1;
      $xmax=$xmax+$deltax/2;
      $xmin=$xmin-$deltax/2;
    }
    if ($deltay < 0.000000001) {
      $deltay = 0.1;
      $ymax=$ymax+$deltay/2;
      $ymin=$ymin-$deltay/2;
    }

    unless ($zoom) {
      $xl = $xmin - 0.05*$deltax;
      $xh = $xmax + 0.05*$deltax;
      $yl = $ymin - 0.05*$deltay;
      $yh = $ymax + 0.05*$deltay;

      # if specific x and y limits were requested, insert them now
      $xl = $limits[0] if (defined $limits[0]) ;
      $yl = $limits[1] if (defined $limits[1]) ;
      $xh = $limits[2] if (defined $limits[2]) ;
      $yh = $limits[3] if (defined $limits[3]) ;
    }

    $colour = 0;
    $symb = 2;
    pgvport(0.1,0.9,0.1,0.9);

    $replot = ();

    PLOT: for ($file=0; $file<$nfiles; ++$file) {
      if (!defined (@{$xx[$file]})) {
        next PLOT;
      }
      @x = @{$xx[$file]}  ;
      @y = @{$yy[$file]}  ;
      #print "x2=@x\n";
      #print "y2=@y\n";

      #pgadvance();
      pgsci(1);
      #pgenv($xl, $xh, $yl, $yh, 0, 0);
      pgsch(1.0);
      unless ($asp) {
        pgswin($xl, $xh, $yl, $yh);
        #pgswin(2005.42, 2005.43, $yl, $yh);
      }else {
        pgwnad($xl, $xh, $yl, $yh);
      }
      unless ($time) {
        #pgtbox ('bctns',0.0,0,'bctns',0.0,0);
        pgtbox ('bctns',0.0,0,'bctns',0.0,0);
      }else {
        pgtbox ('bctnszh',0.0,0,'bctns',0.0,0);
      }

      # plot titles and labels.
      #$xlab = "X";
      #$ylab = "Y";
      #$title = "";
      pglab($xlab, $ylab, $title);
      pgsch(1.0);
      ++$colour;
      ++$symb;
      pgsci($colour);
      $ndata = scalar(@x);
      my ($isline) = 0;
      foreach my $data (@line) {
        if ($data == $file+1) {
          $isline = 1;
        }
      }
      if ($isline == 1) {
        pgline($ndata, \@x, \@y);
      }else {
        pgpoint($ndata, \@x, \@y, $symb);
      }
      if (defined $text[$file]) {
        pgtext($x[$#x], $y[$#y], $text[$file]);
      }
    }

    #pgiden();

    if ($interact) {

      pgsci (1);

      # redraw existing boxes (i.e. those still there from last zoom but not
      # yet applied).
      if (@xbox1) {
        pgsfs(2);
        #print "xbox=@xbox1\n";
        for ($i=0; $i<@xbox1; ++$i) {
          pgrect ($xbox1[$i], $xbox2[$i], $ybox1[$i], $ybox2[$i]);
        }
      }
        
      my ($xcur1, $ycur1, $xcur2, $ycur2, $char) = ();

      #$xcur1 = 0;
      #$ycur1 =0;
      $xcur2 = 0;
      $ycur2 = 0;
      $char = '';
      EDITBOX: while ($char ne 'X') {
        $xcur2 = 0;
        $ycur2 = 0;
        SETCUR: for ($i=0; $i<2; ++$i) {
          $xcur1 = $xcur2;
          $ycur1 = $ycur2;
          pgband (2, 0, $xcur1, $ycur1, $xcur2, $ycur2, $char) 
                    || die "cursor not available for this device";
          $char = uc $char;
          if ($char eq 'C') {
            @xbox1 = ();
            @xbox2 = ();
            @ybox1 = ();
            @ybox2 = ();
            $replot = 1;
            last EDITBOX;
          }
          if ($char eq 'D') {
            $i=-1;
            $xcur2 = 0;
            $ycur2 = 0;
          }
          if ($char eq 'P') {
            $i=-1;
            $xcur2 = 0;
            $ycur2 = 0;
            pgsci (2);
            my (@xpt) = ();
            my (@ypt) = ();
            my ($point) = ();
            my ($npt) = 0;
            pgncur (100, $npt, \@xpt, \@ypt, 4);
            for ($j=0; $j<$npt; ++$j) {
              print "$xpt[$j], $ypt[$j]\n";
            }
            pgsci (1);
          }
          if ($char eq 'U') {
            $zoom = ();
            $replot = 1;
            last EDITBOX;
          }
          if ($char eq 'X') {last EDITBOX}
        }
        if ($xcur2 < $xcur1) {
          ($xcur2,$xcur1) = ($xcur1,$xcur2);
        }
        if ($ycur2 < $ycur1) {
          ($ycur2,$ycur1) = ($ycur1,$ycur2);
        }
        if ($char eq 'A') {
          pgsfs (2);
          pgrect($xcur1, $xcur2, $ycur1, $ycur2);
          push (@xbox1, $xcur1);
          push (@xbox2, $xcur2);
          push (@ybox1, $ycur1);
          push (@ybox2, $ycur2);
        }
        if ($char eq 'Z') {
          $xl = $xcur1;
          $xh = $xcur2;
          $yl = $ycur1;
          $yh = $ycur2;
          $replot = 1;
          $zoom = 1;
          last EDITBOX;
        }
      }
    }

  }
  pgend();

  return \@xbox1, \@xbox2, \@ybox1, \@ybox2 if $interact;
  return ();
}
return 1;



