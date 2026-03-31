<<<<<<< HEAD
#!/local/bin/perl -w
=======
#!/usr/bin/env perl
>>>>>>> pipe2
# Cormac Reynolds: February 2001 - original program
# Simple program to produce the NME feedback pages and provide links to the evn
# pipeline output postscript files
# 18 Dec 2003 - made separate links for each source to selfcal solutions.
# 21 Feb 2006 - link to .pdf instead of .PS.gz. No longer need to do any
#     conversion .
# 10 Dec 2014 - EPB, remove requirement on 'use Shell'
# 30 Sep 2022 - Disabling the mandatory ^D and RTN inputs from user

#use Shell ("date", "convert", "gzip", "gunzip");
use Getopt::Long;

print "use -exp option to input experiment name\n";
print "use -source option to input source names (use quotes for more than one
source). By default will use all sources with plot files in the pwd.\n";
#print "use -gzip=F to not gzip files and have links point to uncompressed PS ",
#      "files\n";
print "use -nme=T to format the page for NMEs, otherwise formatted for user experiments\n";
print "use -jss to specify the contact email to appear on the web page (omit
the '\@jive.eu')\n";
#print "use -pngok if you don't want to remake the png files\n";
print "experiments pipelined before 18 Dec 2003 should use feedback.pl.031218\n";

GetOptions('exp=s'=>\$expname, 'source=s'=>\$sources, 'nme=s'=>\$nme, 
          'jss=s'=>\$jss);
if ($sources) {
  $sources =~ s/^\s+//;
  @sources = split (/\s+/, $sources);
}

unless (defined $jss) {
  print "\n\nEnter your email address (omit the '\@jive.eu')\n";
  chomp ($jss = <STDIN>);
}
unless ($jss =~ /@/) {
  $jss = $jss . '@jive.eu';
}

$nme = uc $nme      if (defined $nme);

unless (defined $expname) {
  print "\n\nWhat is the experiment name?\n";
  chomp ($expname = <STDIN>);
}
unless (defined $sources) {
  @sourcesguess = getsources($expname);
  print "\n\nWhat sources are to be plotted?\n";
  print "By default, will use the following sources: @sourcesguess\n";
  # print "Enter just ^D if the default list is o.k.\n";
  # chomp (@sources = <STDIN>);
}

unless (@sources) {
  @sources = @sourcesguess;
}
foreach (@sources) {
  tr/a-z/A-Z/ ;
}
#nmedir is the directory that everything is in (psfiles, comment file, output
#html file etc.). wwwdir is the path name used
#for the links - it may be a relative path.
#print "\n\nEnter the directory containing the plot files (<RTN> for pwd):\n";
#chomp ($nmedir = <STDIN>);
#if ($nmedir eq '') {
$nmedir = '.';
#}
unless ($nmedir =~ /\/$/) {
  $nmedir = $nmedir . '/';
}
#$wwwdir = ("/juw08_1/scratch/cormac/nme/$expname/");
$wwwdir = ("./");
$htmlfile = ("$nmedir$expname.html");
#print "$htmlfile\n";
open (OUTFILE,">$htmlfile") || die "can\'t open file $!\n";
#opendir (NMEDIR, "$nmedir") || die "can\'t read the NME directory: $!\n";
#@psfiles = grep (/$expname.*\.PS$/, readdir NMEDIR);
#print "@psfiles\n";

#make the basic web page:
print OUTFILE <<EOT
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN"
        "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en" xml:lang="en">
<head>
EOT
;
if (defined $nme && $nme eq 'T') {
  print OUTFILE "<title>European VLBI Network NME Feedback</title>\n";
}else {
  print OUTFILE "<title>European VLBI Network Pipeline Feedback</title>\n";
}
print OUTFILE <<EOT
        <meta http-equiv="Content-Type" content="text/html; charset=iso-8859-1" />
        <link rel="stylesheet"
              type="text/css"
              href="http://old.evlbi.org/css/evn.css"
              title="JIVE style sheet" />

EOT
;
print OUTFILE "</head>\n";
print OUTFILE "<body>\n";
print OUTFILE "\n";
print OUTFILE "\n";
print OUTFILE <<EOT
<div class="totale">


   <div class="content">

      <div class="banner">
                <script type="text/javascript"
                        src="http://old.evlbi.org/javascript/imagecycle4.js">
                </script>
                <noscript>
                <img src="http://old.evlbi.org/graphics/banner/evnbanner.gif" alt="EVN Banner" />
                </noscript>
      </div>

      <div class="main">
EOT
;
#print OUTFILE "<ul>\n";
print OUTFILE "\n";
print OUTFILE "\n";
print OUTFILE "<br />\n";
#print OUTFILE "<center>\n";
if (defined $nme && $nme eq 'T') {
  print OUTFILE "<h1 class=\"title\">EVN Network Monitoring Experiment (NME) Pipeline Feedback </h1>\n";
}else {
  print OUTFILE "<h1 class=\"title\">EVN User Experiment Pipeline Feedback </h1>\n";
}
print OUTFILE "<br />\n";
#print OUTFILE "</center>\n";
print OUTFILE "\n\n";
print OUTFILE "<p>\n";
print OUTFILE "<object>\n";
print OUTFILE "<center>\n";
print OUTFILE "<em>\n";
$expnameup = $expname;
$expnameup =~ tr/a-z/A-Z/;
print OUTFILE "Pipeline feedback for experiment $expnameup. \n";
print OUTFILE "If you have any comments on this experiment please email \n the
                  address below. <br />\n",
                  "A <a
               href=\"http://old.evlbi.org/pipeline/pipe_desc.html\">detailed
                  description of the pipeline output</a> is available. <br />
                  \n ";

print OUTFILE "</em>\n";
#print OUTFILE "The AIPS calibration tables produced by the pipeline can be
#               retrieved from the <a
#               href=\"ftp://vlbeer.ira.bo.cnr.it/vlbi_arch/feb02\">VLBEER ftp
#               server</a>\n";
print OUTFILE "</center>\n";
print OUTFILE "</object>\n";

print OUTFILE "</p>\n\n";
print OUTFILE "<hr />\n";
print OUTFILE "<p>\n";
print OUTFILE "<!-- HTML file last updated: -->\n";
$update = `date`;
print OUTFILE "Last updated: $update\n";
print OUTFILE "<a href=\"mailto:$jss\">$jss</a>\n";
print OUTFILE "\n";
#print OUTFILE "</table>\n";
print OUTFILE "<br /> \n";

#read in the comments file
$commfile = ("$nmedir$expname.comment");
#print "$commfile\n";
if (-e $commfile) {
  open (COMMFILE,"$commfile") ;
  $i=0;
  while (<COMMFILE>) {
    chomp;
    $comment[$i] = $_;
    ++$i;
  }
} else {
  $comment[0] = ' ';
  #print "comment=@comment\n"
}

$commjoin = join (' ', @comment);
@comment = split (/\/\/\//, $commjoin);
#print "@comment";

print OUTFILE "</p>\n\n";
print OUTFILE "<hr /> \n";



$name = "GENERAL";
print OUTFILE "<p>\n";
print OUTFILE "<font color=\"\#4400CC\">\n";
print OUTFILE "General Comments. \n";
print OUTFILE "</font>\n";
$link = ($wwwdir . $expname .  '.DTSUM');
($start_href, $end_href) = mkhref($link);
print OUTFILE "(";
print OUTFILE $start_href;
print OUTFILE "Brief data summary";
print OUTFILE $end_href;
print OUTFILE " and ";
$link = ($wwwdir . $expname .  '.SCAN');
($start_href, $end_href) = mkhref($link);
print OUTFILE $start_href;
print OUTFILE "scan listing";
print OUTFILE $end_href, ") <br />\n";
print OUTFILE getcomm($name);
#print OUTFILE "$thiscomment\n";
#print OUTFILE "<br /> \n";
print OUTFILE "<br /> \n</p>  \n";
#$i=0;
#while ($i<@comment) {
#  print "comment2=$comment[$i]\n";
#  ++$i;
#}

#print OUTFILE "<p>\n";
$name = "ERIVAL";
#print OUTFILE " The EVN reliability indicator (ERI) for this experiment was
#ERI = ", getcomm($name), ". ";
$name = "ERISTAR";
#print OUTFILE "ERI* = ", getcomm($name), ".\n";
$name = "ERICOMM";
#print OUTFILE getcomm($name), "\n";

#print OUTFILE "<br /> \n</p> \n<hr /> \n \n<p> \n";
print OUTFILE "\n<hr /> \n \n<p> \n";
#and put the links to the ps files

wrt_section("POSSM_AUTOCORR", "Plots of the autocorrelations\n");

wrt_section("VPLOT_UNCAL", "Plots of the uncalibrated amplitude and phase
against time");

wrt_section("POSSM_UNCAL", "Plots of the uncalibrated amplitude and phase
against frequency channel\n");

wrt_section("POSSM_CPOL", "The uncalibrated amplitude and phase of the crosshand
correlations against frequency channel\n");

wrt_section("TSYS", "TSYS against time\n");

wrt_section("GAIN", "Telescope sensitivities ", "from the a priori TSYS and Gain
curves (the square of this number gives the antenna noise (SEFD) in Jy - the
smaller the better).");

wrt_section("FRING_PHAS", "Fringe-fit phase solutions ", "(including Parallactic
Angle correction).");

wrt_section("FRING_DELAY", "Fringe-fit delay solutions\n");

wrt_section("FRING_RATE", "Fringe-fit rate solutions\n");

wrt_section("BANDPASS", "Telescope bandpasses\n");

wrt_section("VPLOT_CAL", "Calibrated amplitude and phase against time ", "(a
priori amplitude calibration and fringe-fit solutions applied).");

wrt_section("POSSM_CAL", "Calibrated amplitude and phase against frequency
channel\n");

$nmaps=0;
$name = "IMAPN";
print OUTFILE "Naturally weighted dirty map (not useful for bright sources)
produced before self-cal of: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf', 'FITS');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@suffix) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$nmaps=0;
$name = "IMAPU";
print OUTFILE "Uniformly weighted dirty map (not useful for bright sources)
produced before self-cal of: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf', 'FITS');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@suffix) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$name = "CALIB_PHAS1";
print OUTFILE "Phase corrections applied to a priori calibrated and
fringe-fitted data by self-calibration. <br />\n";
$nmaps = 0;
while ($nmaps < @sources) {
  @suffix = ('pdf');
  @text = ("$sources[$nmaps]");
  wrt_map_section (\$name, \$nmaps, \@suffix, \@text) ;
  ++$nmaps;
}
wrcomm($name);

$name = "CALIB_AMP2";
print OUTFILE "Amplitude corrections applied to a priori calibrated and
fringe-fitted data by self-calibration. <br />\n";
$nmaps = 0;
while ($nmaps < @sources) {
  # do the amplitude calibration summary
  $link = ($wwwdir . $expname . "_" . $sources[$nmaps] . '_' . $name . ".TXT" );
<<<<<<< HEAD
  if (-d "/aps3/Pypeline/reynolds"){
    #system "~reynolds/progs/perl/ampcalstats.pl $link" ;
    system "/aps3/Pypeline/reynolds/perl/ampcalstats.pl $link" ;
  }
=======
  #if (-d "/aps3/Pypeline/reynolds"){
    #system "~reynolds/progs/perl/ampcalstats.pl $link" ;
  system "/home/jops/opt/evn_support/ampcalstats.pl $link" ;
  #}
>>>>>>> pipe2
  @suffix = ('pdf', 'TXT', 'ampcal');
  @text = ('pdf', 'text file', 'statistical summary');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@text) ;
  ++$nmaps;
}
wrcomm($name);

$name = "SENS";
wrt_section("SENS", "Telescope sensitivities", " (the total AMP gain applied
during both a priori and self calibration; the square of this number gives the
antenna noise (SEFD) in Jy).\n");

$nmaps=0;
$name = "CLPHS";
print OUTFILE "Residual closure phase (visibility closure phase with model",
            " closure phase subtracted) for: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf');
  @text = ("$sources[$nmaps]");
  #print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@text) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$nmaps=0;
$name = "VPLOT_MODEL";
print OUTFILE "Calibrated visibilities and the source model of: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf');
  @text = ("$sources[$nmaps]");
  wrt_map_section (\$name, \$nmaps, \@suffix, \@text) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$nmaps=0;
$name = "UVPLT";
print OUTFILE "Calibrated visibilities against <em>u,v</em> distance for: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf', 'png');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@suffix) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$nmaps=0;
$name = "UVCOV";
print OUTFILE "<em>u,v</em> coverage for: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf', 'png');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@suffix) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}

$nmaps=0;
$name = "ICLN";
print OUTFILE "<em>Crude</em> maps of sources: <br />";
while ($nmaps < @sources) {
  @suffix = ('pdf', 'FITS');
  print OUTFILE "$sources[$nmaps]:";
  wrt_map_section (\$name, \$nmaps, \@suffix, \@suffix) ;
  ++$nmaps;
}
if ($nmaps > 0) {
  wrcomm($name);
}


print OUTFILE "</p>\n\n";
print OUTFILE "\n";
print OUTFILE "\n";
#print OUTFILE "</center>\n";
#print OUTFILE "</ul>\n";
print OUTFILE "</div>\n";
print OUTFILE <<EOT
<!--#include virtual="./foot.html"-->

EOT
;
print OUTFILE "</div>\n";
print OUTFILE "</div>\n";

print OUTFILE "</body>\n";
print OUTFILE "</html>\n";

chmod 0755, $htmlfile;


#sub mkimg {
#  my ($psname) = $_[0];
#  my ($pngname) = $psname;
#  $pngname =~ s/PS/png/;
#  if (-e $psname) {
#    print "converting $psname to $pngname\n";
#    #pstoimg ("$psname");
#    convert ("$psname $pngname");
#  }elsif (-e "$psname.gz") {
#    gunzip ("$psname.gz") ;
#    print "converting $psname.gz to $pngname\n";
#    #pstoimg ("$psname");
#    convert ("$psname $pngname");
#  }
#}

#sub zipit {
#  my $file = $_[0];
#  if ($gz) {
#    if (-e $file) {
#      print "gzipping $file\n";
#      gzip ("$file")  ;
#    }
#    $file .= $gz;
#  }
#  return $file;
#}

sub mkhref {
  my ($link) = $_[0];
  my ($start_href, $end_href) ;
  if (-e $link) {
    $start_href = "<a href =\"$link\">\n";
    $end_href = "</a> \n";
  }else {
    $start_href = "";
    $end_href = "<font color=\"\#FF0000\"> (not available) </font> \n";
  }
  return $start_href, $end_href;
}

sub wrcomm {
  my ($name) = $_[0];
  print OUTFILE "<font color=\"\#4400CC\">\n";
  print OUTFILE "Comments. <br /> \n";
  print OUTFILE "</font>\n";
  $thiscomment = getcomm($name);
  #@thiscomm = grep (/^ *$name/, @comment);
  #$thiscomment = join (' ', @thiscomm);
  #$thiscomment =~ s/^ *$name//g;
  print OUTFILE "$thiscomment\n";
  print OUTFILE "<br /> \n</p> \n<hr /> \n \n<p> \n";
}

sub getsources {
  my ($expname) = $_[0];
  opendir (THISDIR, ".") || die "can\'t read the current directory: $!\n";
  my (@sourcesguess) = grep (/$expname.*_UVPLT.pdf/, readdir THISDIR);
  foreach my $source (@sourcesguess) {
    $source =~ s/$expname\_(.*)_UVPLT.pdf.*/$1/;
  }
  closedir (THISDIR);
  return @sourcesguess;
}

sub getcomm {
  my ($name) = $_[0];
  @thiscomm = grep (/^ *$name/, @comment);
  $thiscomment = join (' ', @thiscomm);
  $thiscomment =~ s/^ *$name//g;
  return $thiscomment;
}

sub wrt_section{
  my ($name, $text, $text2) = @_;
  $link = ($wwwdir . $expname . "_" . $name . ".pdf" );
  #$link=zipit($link);
  ($start_href, $end_href) = mkhref($link);
  print OUTFILE $start_href;
  print OUTFILE $text;
  print OUTFILE $end_href;
  print OUTFILE $text2 if $text2;
  print OUTFILE "<br />\n";
  wrcomm($name);
}


sub wrt_map_section{
  my ($name, $nmaps, $suffix, $text) = @_;
  $name = ${$name};
  $nmaps = ${$nmaps};
  @suffix = @{$suffix};
  @text = @{$text};
  for (my $i=0; $i<@suffix; ++$i) {
    # first the ps file
    $link = ($wwwdir . $expname . "_" . $sources[$nmaps] . "_" . $name .
        '.' . $suffix[$i]);
    #$link=zipit($link);
    ($start_href, $end_href) = mkhref($link);
    print OUTFILE " ";
    print OUTFILE $start_href;
    print OUTFILE "$text[$i]";
    print OUTFILE $end_href;
    if ($i+1 < scalar(@suffix)) {
      print OUTFILE ", or \n";
    }else {
      print OUTFILE ". <br />\n";
    }
  }

}


