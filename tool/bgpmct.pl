#!/usr/bin/perl

## --| Description |--------------------------------------------------------- ##
##
## Vamsi	- Original MCT algorithm
##
## +Bing	- (Bottom search.backward) for more accurate start time
##		- (Bottom search.forward ) for more accurate end time
## +Rachita	- (One RIB per day) for more accurate estimate of RIB size
## +Jie		- (One RIB per day) for more accurate estimate of RIB size
##
## -------------------------------------------------------------------------- ##

use warnings;
use strict;
use Pod::Usage;
use Getopt::Long;

if($#ARGV < 1) {
  pod2usage(2);
}

my $progname = "bgpmct.pl";

my $error = -1;

## valid field names allowed in Field:Index pairs
my $validFieldIndex = "PEER,PREFIX,TS,AWS"; #!! DO NOT TOUCH THIS LINE !!

## system-wide configurable parameters and options (Defaults)
my %options = ( 
		# Options for parsing MRT ASCII files
                'fieldsep'     => '\|',   # default input field seperator
                'fieldindex'   => "TS:2,AWS:3,PEER:4,PREFIX:6", # default input field indices

		# Options for detecting table transfer (MCT)
                'percent'      => 0.99,   # 'N'  table size fraction 
                'range'        => 7200,   # 'U'  upper bound on s(t)
                'b_x'          => 30,     # 'Bx' bottom search threshold
                'b_s'          => 3,      # 'SE' bottom search for start
                'b_e'          => 3,      # 'BE' bottom search for end
              );

## command line options.
my %cmdLOptions = ( 
                    # General options
                    "help|?"           => \$options{'help'},
                    "debug|d:s"        => \$options{'debug'},

                    # Command line options for table transfer		   
                    "fieldindex|fi=s"       => \$options{'fieldindex'},
                    "validate"              => \$options{'validate'},

                    # Command line options for input files   
                    "ribfile|rf=s"          => \$options{'ribfile'},
                    "updatefileslist|ul=s"  => \$options{'updatefileslist'},
                    "peer|p=s"              => \$options{'peer'},
                    "ctimefile|cf=s"        => \$options{'ctimefile'},       # ouput file 

                    # Command line options for table transfer		   
                    "percent|N=f"           => \$options{'percent'},
                    "range|U=i"             => \$options{'range'},
                    "bottomsearch|B=i"      => \$options{'b_x'},
                    "bottomsearchStart|SE=i"  => \$options{'b_s'},
                    "bottomsearchEnd|BE=i"    => \$options{'b_e'},
                  );

Getopt::Long::Configure("no_bundling","no_ignore_case");
## Get command line options. 
my $result = GetOptions(%cmdLOptions);
pod2usage(1) if $options{'help'};
pod2usage(2) if !$result or ($#ARGV >= 0);
pod2usage(2) if !defined($options{'updatefileslist'}) or
                !defined($options{'peer'})            or
                ((!defined($options{'ribfile'}))       and
                (!defined($options{'ribfileslist'})));

## "Debug" switches
my ( $DEBUG_CTIME,   # print collection times.
     $DEBUG_DETECTD, # print all local minima from collection times
     $DEBUG_OVERLAP, # print detector output after overlap removal
     $DEBUG_VERBOSE
   ) = (0,0,0,0);

## set debugging flags
if(defined($options{'debug'})) {
  ($DEBUG_CTIME, $DEBUG_DETECTD, $DEBUG_OVERLAP, $DEBUG_VERBOSE) = (1,1,1,1)
                                                                   if $options{'debug'} eq '' or $options{'debug'} =~ m/a/i;
  $DEBUG_CTIME   = 1 if $options{'debug'} =~ m/c/i;
  ## note: you can always save the ctime file by specifying "-cf ctime.txt"
  ## on command line.
  $DEBUG_DETECTD = 1 if $options{'debug'} =~ m/d/i;
  $DEBUG_OVERLAP = 1 if $options{'debug'} =~ m/o/i;
  $DEBUG_VERBOSE = 1 if $options{'debug'} =~ m/v/i;
}

## --| Main |---------------------------------------------------------------- ##

## parse Field:Index pairs list (and figure out various fields from input data)
my %fieldIndex = &parseFieldIndex($options{'fieldindex'}, $validFieldIndex);
if (!%fieldIndex) {
  &logError($progname, "Errors occured while parsing field:index string");
  exit($error);
} 

## ---------------------------------------------
## validate user input: peer, percent, and range
## ---------------------------------------------
#sxr#
=dop
## valid peer
my $rx_ipnum   = qr{([01]?\d\d?|2[0-4]\d|25[0-5])}; # quad of IP address
my $rx_ipaddr  = qr{^($rx_ipnum\.){3}$rx_ipnum$};   # IP address
my $rx_ipaddr6 = qr{^(\w{0,4}\:)*\w{0,4}$};   # IP address
if ($options{'peer'} !~ m/$rx_ipaddr/ and $options{'peer'} !~ m/$rx_ipaddr6/) {
  &logError($progname, "Invalid Peer: $options{'peer'} specified on input");
  exit($error);
}
=cut


## invalid percent
#if ($options{'percent'} < 0 || $options{'percent'} > 1) {
if ($options{'percent'} < 0 ) {
  &logError($progname, "Invalid Table size fraction: $options{'percent'}");
  exit($error);
}
## invalid range
if ($options{'range'} < 0) {
  &logError($progname, "Invalid Max s(t): $options{'range'}");
  exit($error);
}
## invalid bottomsearch interval
if ($options{'b_x'} < 0) {
  &logError($progname, "Invalid Bottom Search interval: $options{'b_x'}");
  exit($error);
}
## invalid bottomsearch interval
if ($options{'b_s'} < 0) {
  &logError($progname, "Invalid Bottom Search interval: $options{'b_s'}");
  exit($error);
}
## invalid bottomsearch interval
if ($options{'b_e'} < 0) {
  &logError($progname, "Invalid Bottom Search interval: $options{'b_e'}");
  exit($error);
}

my $expectedTableSize = 0;


## check if user supplied correct updatefileslist file
if (! -e $options{'updatefileslist'}) {
  &logError($progname, "Unable to find updatefileslist file ". "$options{'updatefileslist'}: file does not exist");
  exit($error);
}
## check if it is a plain text file
if (! -T $options{'updatefileslist'}) {
  &logError($progname, "Invalid updatefileslist file ".  "$options{'updatefileslist'}: not a plain text file");
  exit($error);
}
## check if it is empty
if (-z $options{'updatefileslist'}) {
  &logError($progname, "Invalid updatefileslist file ".  "$options{'updatefileslist'}: file is empty");
  exit($error);
}
#to calculate pragram run time
#my $begin_time = time();
## ---------------------------------------------
## collection times: all peer
## %ctime ===> key==>[ts, s(t)]
## - Calculate all collection times
## ---------------------------------------------
my %ctimes;
if (!defined($options{'ctimefile'})) {
  my $status = &getCollectionTimes(
                                    $options{'ribfile'},
                                    $options{'updatefileslist'}, 
                                    \%ctimes,
                                    $options{'range'}, 
                                    \%fieldIndex,
                                  );
  if ($status) {
    &logError($progname, "Errors occured while generating collection time(s)");
    exit($error);
  }
}
else {
## user requested for ctimes file (Complete collection time log)
  my $status = &getCollectionTimes(
                                    $options{'ribfile'},
                                    $options{'updatefileslist'}, 
                                    \%ctimes, 
                                    $options{'range'}, 
                                    \%fieldIndex,
                                    $options{'ctimefile'}
                                  );
  if ($status) {
    &logError($progname, "Errors occured while generating collection time(s)");
    exit($error);
  }
}

while(my($key,$value) = each %ctimes)
{

  ## ---------------------------------------------
  ## local minima list: 
  ## localMinima[i] ==> ref->[ts, s(t), index_into_ctimes]
  ## - Locate local minima of collection times
  ## ---------------------------------------------
  my @localMinima;
  if (&getLocalMinimaList($value, \@localMinima)) 
  {
    &logError($progname, "Errors occured while getting local minima list");
    exit($error);
  }
=dop
    print $key;
    print "\n";
    my $value_num = scalar keys @localMinima;
    #print "value_num: $value_num \n";
    my $count = 0;
    while($count<$value_num)
    {
      #print "count: $count\n";
      print "$localMinima[$count]->[0]  $localMinima[$count]->[1]  $localMinima[$count]->[2]\n";
      $count++;
    }
=cut
  ## print all local minima list if debug_detectd is set
  if ($DEBUG_DETECTD) {
   &debug($progname, "All local minima:");
    foreach my $lcmin (@localMinima) {
      #&debug($progname, "$$lcmin[0]   $$lcmin[1]");
      &debug($progname, sprintf("%s   %s   %s", $$lcmin[0], $$lcmin[1], $$value[$$lcmin[2]]->[2]));
    }
  }

  ## ---------------------------------------------
  ## detected local minima: 
  ## detected[i] ==> ref->[ts, s(t), index_into_ctimes]
  ## - Remove overlapped local minima
  ## ---------------------------------------------
  my @detected;
  if (&removeOverlap(\@localMinima, \@detected)) 
  {
    &logError($progname, "Errors occured while removing overlaps");
    exit($error);
  }
  ## there might be no table transfers! (after overlap elimination)
  if (!@detected) {
    &debug($progname, "No table transfers found!") if $DEBUG_VERBOSE;
    next;
 }
  ## print overlap eliminated local minima list if debug_overlap is set
  if ($DEBUG_OVERLAP) {
    &debug($progname, "Local minima after overlap elimination:");
    foreach my $overlap (@detected) {
      &debug($progname, "$$overlap[0]   $$overlap[1]");
    }
  }

  #sxr#test
=dop
  my $det_num = scalar keys @detected;
  my $count_det =0;
  while($count_det < $det_num)
  {
    print "$detected[$count_det]->[0] $detected[$count_det]->[1]  $detected[$count_det]->[2]\n";
    $count_det++;
  }
=cut
  ## ---------------------------------------------
  ## bottom search: 
  ## bottomsearch[i] ==> ref->[ts, s(t)]
  ## - Bottom search to refine table transfer estimation 
  ## ---------------------------------------------
  my @bottomSearched;
  my @endPoint;
  
  if (&bottomSearch($value, \@detected, \@bottomSearched, $options{'b_x'}, $options{'b_s'}, $options{'b_e'}, \@endPoint)) {
    &logError($progname, "Errors occured while bottom searching");
    exit($error);
  }
  
  ## print table transfers: [t, s(t)] pairs
  #print "bottomSearch:";
  #foreach my $rf (@bottomSearched) {
  #  print "$rf->[0]  $rf->[1]\n";
  #}
  print "$key:\n";
  ## print endpoint of table transfer [endTS, duration, t1, s(t1), diff_s2s1]
  print "#", join(",", "START", "END", "CTIME",
                     "PRE_TS", "PRE_ST", "DIFF_ST_PRE",
                     "SUC_TS", "SUC_ST", "DIFF_ST_SUC",
                     "RIBSIZE",
               ), "\n";
  #print STDERR Dumper(@endPoint);
  foreach my $ep (@endPoint){
    #print "$ep->[0]  $ep->[1]  $ep->[2]  $ep->[3]  $ep->[4]  $ep->[5]\n";
    print join(",", @{$ep}), "\n";  
    }
}
#sxr# test
#my $end_time = time();
#my $runtime;
#$runtime = $end_time - $begin_time;
#print "run time: $runtime\n";

exit;


## -------------------------------------------------------------------------- ##
## Functions
## -------------------------------------------------------------------------- ##

## --| RIB Processing |------------------------------------------------------ ##
sub getTableSize {
## input:
##  arg 1: string, representing ribfile name 
##  arg 2: string, representing peerlist name   #sxr# 
##  arg 3: reference to hash, representing Field:Index pairs
## output:
##  int, positive integer representing table size
##  (OR) -1 on error
## description:
##  return table size (from ribfile) for a given peer

  my $name = "$progname:getTableSize";
  my %ribPrefixes;
  my %peer_prefix;
  my $error = -1;
  our @rfcpeer;
  ## 3 parameters required
  if (scalar @_ != 3) {
    &logError($name, "Expecting 3 arguments, but received (@_) instead");
    return $error;
  }

  my ($ribFile, $peerlist, $rfFieldIndex) = @_;
  
  #sxr# open peerlist.txt(one peer address one line)
  if (!open(PEERLIST,"$peerlist")) 
  {
    &logError($name, "Unable to open ribfile: $ribFile for reading: $!");
    return $error;
  }
  my $line_peer = <PEERLIST>;
  if (!defined($line_peer)) 
  {
    &logError($name, "Invalid ribfile $peerlist: ribfile is empty");
    return $error;
  }
  chomp($line_peer);
  push(@rfcpeer,$line_peer);
  while($line_peer = <PEERLIST>)
  {
   chomp($line_peer);
   push(@rfcpeer,$line_peer); 
  }

  if (! -e $ribFile) {
    &logError($name, "Unable to find ribfile $ribFile: file does not exist");
    return $error;
  }
  if (-z $ribFile) { # 0 byte compressed file!
    &logError($name, "Invalid compressed ribfile $ribFile: ". "0 byte file");
    return $error;
  }
  print "$ribFile\n";
  &debug($name, "Using ribfile: $ribFile") if $DEBUG_VERBOSE;
  if (!open(RIBFILE, "gzip -dc $ribFile |")) {
    &logError($name, "Unable to open ribfile: $ribFile for reading: $!");
    return $error;
  }
  &debug($name, "Opened ribfile: $ribFile for reading") if $DEBUG_VERBOSE;
	
  my $line = <RIBFILE>;
  if (!defined($line)) {
    &logError($name, "Invalid ribfile $ribFile: ribfile is empty");
    return $error;
  }
  chomp($line);

  ## we validate the first line of input stream to verify if user supplied
  ## correct input (based on various fields)
  if (!&isValidDataStream($line, $rfFieldIndex)) {
    &logError($name, "Input data does not seem to be in the right format; ".
        "Check documentation for the correct format");
    return $error;
  }
  &debug($name, "Validate ribfile input stream: success") if $DEBUG_VERBOSE;
  #sxr#get peer and prefix from line of rib
  my ($peer, $prefix) = (split($options{'fieldsep'}, $line, 7))[$$rfFieldIndex{'PEER'}, $$rfFieldIndex{'PREFIX'}];
  #sxr#if peer in @rfcpeer, calculate the peer corresponding prefix count 
  if ($peer ~~ @rfcpeer)
  {
   ${$peer_prefix{$peer}}{$prefix}= 0;
  } 

  while($line = <RIBFILE>) {
    chomp($line);

    if ($options{'validate'}) {
    ## validate entire input stream (on user request)
      if (!&isValidDataStream($line, $rfFieldIndex)) {
        &logError($name, "Invalid data found at $. in input stream: ".
            " ignoring this line");
        next;
      }
    }

    ($peer, $prefix) = (split($options{'fieldsep'}, $line, 7))
                       [$$rfFieldIndex{'PEER'}, $$rfFieldIndex{'PREFIX'}];
    if ($peer ~~  @rfcpeer)
    { 
      ${$peer_prefix{$peer}}{$prefix}= 0;
    }
  }
  close(RIBFILE);


  my $peernum = scalar @rfcpeer;
  my $count = 0;
  our %tableSize;
  while ($count<$peernum)
  {
  $peer = $rfcpeer[$count];
  $count++;
  $tableSize{$peer} = scalar keys %{$peer_prefix{$peer}};
  &debug($name, "Found table size for $peer from $ribFile: $tableSize{$peer}") if $DEBUG_VERBOSE;
  }
   
  &debug($name, "Success") if $DEBUG_VERBOSE;
  return %tableSize;
} # end sub getTableSize()

## -------------------------------------------------------------------------- ##

## --| Collection Times |---------------------------------------------------- ##
sub getCollectionTimes {
## input:
##  arg 1: string, representing ribfiles
##  arg 2: string, representing updatefiles list
##  arg 3: reference to list, representing collection times
##                    (ref->ctimes[i] ===> ref->[ts, s(t)])
##  arg 4: int, representing 'range' seconds (upper bound on s(t) values)
##  arg 5: reference to hash, representing Field:Index pairs
##  arg 6: [optional] string, representing ctimesfile (for debugging)
## output:
##  implicit return of collection times (via arg2)
##  0 on success, positive integer on failure
## description:
##  compute [t, s(t)] from each update file from updatefiles list. Collection
##  times are given by individual s(t) values. Note that each update carries a
##  timestamp, and we only compute collection times for a single timestamp (no
##  matter how many updates happen within one timestamp).

  ## function name used in error and debug messages
  my $name     = "$progname:getCollectionTimes";
  my $success  = 0;
  my $error    = 1;
  my $ctimesFH = undef;
  my %range_num;
  my $flag =0;
  ## 7 or 8 parameters required
  if (scalar @_ < 5 && scalar @_ > 6) {
    &logError($name, "Expecting 5 or 6 arguments, but received (@_) instead");
    return $error;
  }
  my ($ribFile, $updateFilesListFile, $rfCtimes, $range, $rfFieldIndex, $ctimeFile);
  ##----------------------------------------------------------
  ## Read in parameters
  ##----------------------------------------------------------
  if (scalar @_ == 5) {
    ($ribFile, $updateFilesListFile, $rfCtimes,  $range, $rfFieldIndex,) = @_;
    $ctimesFH = *STDERR if $DEBUG_CTIME;
  }
  else {
    ($ribFile, $updateFilesListFile, $rfCtimes, $range, $rfFieldIndex, $ctimeFile) = @_;
    ## user requested for writing to ctimesfile
    $DEBUG_CTIME = 1;
    if ($ctimeFile eq "-") {
      ## requested ctimesfile is stderr
      $ctimesFH = *STDERR; 
    }
    else {
      if (!open(CTIMES, "> $ctimeFile")) {
        ## if we fail to open ctimesfile, use stderr instead
        &debug($name, "Unable to open ctimes file: $ctimeFile for writing; ". "Using STDERR instead") if $DEBUG_VERBOSE;
        $ctimesFH = *STDERR;
      }
      else {
        &debug($name, "Opened ctimes file: $ctimeFile for writing") if $DEBUG_VERBOSE;
        $ctimesFH = *CTIMES;
      }
    }
  }

  ##----------------------------------------------------------
  ## Prepare update files list
  ##----------------------------------------------------------
  my @updateFilesList; # list of update files to be processed

  if (!open(UPDFLIST, "< $updateFilesListFile")) {
    &logError($name, "Error opening updatefileslist file: ".  "$updateFilesListFile: $!");
    return $error;
  }
  &debug($name, "Opened updatefileslist file: $updateFilesListFile for reading") if $DEBUG_VERBOSE;

  while (my $file = <UPDFLIST>) {
    chomp($file);
    $file =~ s/\s*$//; # strip whitespace at end of filename
    if (! -e $file) {
      &logError($name, "Unable to find an update file $file ".  "(from updatefiles list): File does not exist");
      return $error;
    }

    if (-z $file) { # 0 byte compressed file!
      &logError($name, "Invalid compressed updatefile $file: ".  "0 byte file");
      return $error;
    }
    push(@updateFilesList, $file);
  }
  close (UPDFLIST);

  ## updatefileslist file may list update files in any order,
  ## but we need them in cronological order
  @updateFilesList = sort(@updateFilesList);

  ##----------------------------------------------------------
  ## MCT processing
  ##----------------------------------------------------------
  my (%start, %end, %uniq_size); # start and end time markers
  my %timestamp;                 # the timestamp of current update
  my %updatePrefixes;            # unique update prefixes
  my %queue;                     # temporary FIFO queue (since perl hash
                                 # does not preserve order)
  my $nRIB = 0;
  my %ribTableSize; 
  %ribTableSize = &getTableSize($ribFile, $options{'peer'}, \%fieldIndex);
  ##test#sxr#
  #while(my($key,$value) = each %ribTableSize )
  #{
  #  print "$key    $value\n";
  #}	
  &debug($name, "[ribFile] $ribFile")        if $DEBUG_VERBOSE;   
  foreach my $updateFile (@updateFilesList) {
      &debug($name, "[updateFile] $updateFile")  if $DEBUG_VERBOSE;
       
    if (!open(UPDFILE, "gzip -dc $updateFile |")) {
      &logError($name, "Unable to open updatefile $updateFile: $!");
      return $error;
    }
    &debug($name, "Opened updatefile $updateFile for reading") if $DEBUG_VERBOSE;
    
    #sxr#test
    print "$updateFile\n";
    # [DEBUG #1]
    my $is_firstline = 1;
    my $line = undef;
    my ($ts, $aws, $peer, $prefix); 
    ## process updates and generate collection times
    while($line = <UPDFILE>) {
      #my %line_count;
      chomp($line);
      next if $line eq ''; #TODO CM
      ($ts, $aws, $peer, $prefix) = (split($options{'fieldsep'}, $line, 7))
                                    [ $$rfFieldIndex{'TS'},
                                      $$rfFieldIndex{'AWS'},
                                      $$rfFieldIndex{'PEER'},
                                      $$rfFieldIndex{'PREFIX'}
                                    ];  
      # [DEBUG #1]

      if ($is_firstline)
      ## If first line
      {
        if (!defined($line)) {
          &debug($name, "Found empty updatefile $updateFile: ignoring") if $DEBUG_VERBOSE;
          close(UPDFILE); 
          ## Continute to the next file
          last;
        }
        ## we validate the first line of input stream to verify if user supplied
        ## correct input (based on various fields)
        if (!&isValidDataStream($line, $rfFieldIndex)) {
          &logError($name, "Invalid update stream supplied as input");
          ## Exit the program
          return $error;
        }
        $is_firstline = 0;    
      }
      else
      ## Not first line
      {
        ## For each line
        if ($options{'validate'}) {
          ## validate entire input stream (on user request)
          if (!&isValidDataStream($line, $rfFieldIndex)) {
            &logError($name, "Invalid data found at $. in input stream: ". " ignoring this line");
            next;
          }
        }
      }
      our @rfcpeer;
      #sxr#ignore 'STATE' and 'W'ithdraw messages
      next if $aws eq 'STATE' or $aws eq 'W';
      #sxr#ignore peers not in peerlist
      next if(($peer ~~ @rfcpeer)==0);
      #sxr#if start{$peer} not intital
      if((exists $start{$peer}) == 0)
      {
        $start{$peer} = $end{$peer} = $timestamp{$peer} = $ts;
      }
      if((exists $range_num{$peer}) == 0)
      {
          $range_num{$peer} = 0;
      }
      #sxr# assignment
      $timestamp{$peer} = $ts;
      #$line_count{$peer}++;
      ## time resolution of updates is in seconds (and it does'nt matter
      ## which updates occurred first within a single second). Hence we 
      ## keep processing updates that occur within a single time stamp.
      ## [DEBUG #2]
      ## If $ts > $end, trigger the calculation of collection time
      if ($timestamp{$peer} > $end{$peer})
      {
        $uniq_size{$peer} = scalar keys $updatePrefixes{$peer};
        #next if $uniq_size < $nRIB and ($ts - $start) < $range;
        $nRIB = $ribTableSize{$peer} * $options{'percent'};
=hop
        if ($DEBUG_VERBOSE)
        {
          #printf STDERR ("%10.10s %10.10s %2.3f\n", $uniq_size,  $nRIB, $uniq_size / $nRIB);
          if ($uniq_size >= $nRIB * 0.8)
          {
            printf STDERR ("%s  %s %s / %s (%2.3f)\n", 
                           ${$queue[0]}[0],
                           $end - $start + 1,
                           $uniq_size,  $nRIB, $uniq_size / $nRIB); 
          }
        }
=cut
        #my $flag =0;
        if ($uniq_size{$peer} >= $nRIB or ($end{$peer} - $start{$peer}) >= $range)
        {
          ##
          ## current updatePrefixes > RIB estimate
          ##
          while(($uniq_size{$peer} >= $nRIB) && (($end{$peer} - $start{$peer}) < $range)) {
            ## advance "start" marker to the next timestamp in queue
            while(${$queue{$peer}}[0][0] == $start{$peer}) {
              my $que_ref = shift(@{$queue{$peer}});
              ${$updatePrefixes{$peer}}{$$que_ref[1]}--;
              delete ${$updatePrefixes{$peer}}{$$que_ref[1]} if ${$updatePrefixes{$peer}}{$$que_ref[1]} == 0;
            }

            push(@{$$rfCtimes{$peer}}, [$start{$peer},$end{$peer} - $start{$peer} + 1, $uniq_size{$peer}]); ## [DEBUG #0: Time granularity] 
            print $ctimesFH ("$start{$peer}  ", $end{$peer} - $start{$peer} + 1, "\n") if $DEBUG_CTIME;
            $flag = 1;
            $range_num{$peer} = 0;
            # Reset $start and exit if @queue is empty
            #sxr#
            #next if (scalar(@{$queue{$peer}}) <=0);
            if ( scalar(@{$queue{$peer}}) <=0 ) { $start{$peer} = undef; last; }
            $start{$peer} = ${$queue{$peer}}[0][0];
            $uniq_size{$peer} = scalar keys %{$updatePrefixes{$peer}};
          }

          ##
          ## collectiontime > maximum range
          ##
          while(($end{$peer} - $start{$peer}) >= $range) {
            ## advance "start" marker to the next timestamp in queue
            while(${$queue{$peer}}[0][0] == $start{$peer}) {
              my $que_ref = shift(@{$queue{$peer}});
              ${$updatePrefixes{$peer}}{$$que_ref[1]}--;
              delete ${$updatePrefixes{$peer}}{$$que_ref[1]} if ${$updatePrefixes{$peer}}{$$que_ref[1]} == 0;
            }
            #push(@$rfCtimes, [$start,$range]);
            if((($range_num{$peer} <= 50) and ($flag ==0)) or ($flag ==1))
            {
             push(@{$$rfCtimes{$peer}}, [$start{$peer},$range, $uniq_size{$peer}]);
             print $ctimesFH ("$start{$peer}  ", $range, "\n") if $DEBUG_CTIME;
             #$range_num{$peer}++;
            }
            $range_num{$peer}++;
            $flag = 0; 
            # Reset $start and exit if @queue is empty
            if ( scalar(@{$queue{$peer}}) <=0 ) 
             { $start{$peer} = undef; 
               last; 
             }
            $start{$peer} = ${$queue{$peer}}[0][0];
          }

        }
      }
      ## Insert update quque and unique prefix table
      push(@{$queue{$peer}}, [$ts, $prefix]); # enqueue updates
      ${$updatePrefixes{$peer}}{$prefix}++;
      ## Advance end time marker
      $end{$peer} = $ts;

      ## initilize start and end time markers
      if (!defined($start{$peer})) { #TODO CM
         $start{$peer} = $end{$peer};
      }
    } # end while($line = <UPDFIL..
    close(UPDFILE);
    
  }

 &debug($name, "Success") if $DEBUG_VERBOSE;
  return $success;
} ## end sub getCollectionTimes()


## -------------------------------------------------------------------------- ##
## --| detector |------------------------------------------------------------ ##
sub getLocalMinimaList {
## input:
##  arg 1: reference to list, representing collection times
##         (ref->ctimes[i] ==> ref->[ts, s(t)]) 
##  arg 2: reference to list, representing local minima list
##         (ref->localMinima[i] ==> ref->[ts, s(t), index_into_ctimes])
## output:
##  implicit return (through arg2) of local minima
##  0 on success, positive integer on error
## description:
##  find all local minima from collection times. Local minimum is any
##  [ts_x, s(t)_x] pair that has the following property:
##    s(t)_x-1 > s(t)_x < s(t)_x+1

  ## function name used in error and debug messages
  my $name = "$progname:getLocalMinimaList";
  my $success = 0;
  my $error = 1;
 
  ## 2 parameter required
  if (scalar @_ != 2) {
    &logError($name, "Expecting 2 arguments, but received (@_) instead");
    return $error;
  }
 
  my ($rfCtimes, $rfLocalMinima) = @_;
 
  ## UPHILL = 0, DOWNHILL = 1, upOrDown is initially set to DOWNHILL (1)
  my ($upOrDown, $UPHILL, $DOWNHILL) = (1, 0, 1);
  ## current and previous [ts, s(t)] pair
  my ($curr_ts, $curr_st, $prev_ts, $prev_st);
 
  $prev_ts = $rfCtimes->[0][0]; $prev_st = $rfCtimes->[0][1];
  for (my $i=1; $i <= $#$rfCtimes; $i++) {
    $curr_ts = $rfCtimes->[$i][0]; $curr_st = $rfCtimes->[$i][1];
 
    ## when downhill:
    if(($upOrDown == $DOWNHILL) && ($curr_st > $prev_st)) {
      ## prev_[ts, s(t)] pair is a local minimum
      push (@$rfLocalMinima, [$prev_ts, $prev_st, $i-1]);
      $upOrDown = $UPHILL;
    }
    ## when uphill:
    if(($upOrDown == $UPHILL) && ($curr_st < $prev_st)) {
      ## we are starting to go downhill
      $upOrDown = $DOWNHILL;
    }
    $prev_ts = $curr_ts; $prev_st = $curr_st;
  }
 
  &debug($name, "Success") if $DEBUG_VERBOSE; 
  return $success;
} ## end sub getLocalMinimaList

## -------------------------------------------------------------------------- ##

## --| Overlap Elimination |------------------------------------------------- ##
sub removeOverlap {
## input:
##  arg 1: reference to list, representing local minima list
##         (ref->localMinima[i] ==> ref->[ts, s(t), index_into_ctimes])
##  arg 2: reference to list, representing local minima after overlap removal
##         (ref->detected[i] ==> ref->[ts, s(t), index_into_ctimes])
## output:
##  implicit return (through arg2) of overlap removed local minima
##  0 on success, positive integer on error
## description:
##  generate a list of overlap removed local minima. If no local minima exist,
##  return a null list in arg2.

  ## function name used in error and debug messages
  my $name = "$progname:removeOverlap";
  my $success = 0;
  my $error = 1;
 
  ## 2 parameter required
  if (scalar @_ != 2) {
    &logError($name, "Expecting 2 arguments, but received (@_) instead");
    return $error;
  }
 
  my ($rfLocalMinima, $rfDetected) = @_;
  my ($curr_lref, $prev_dref);
 
  ## return null list in arg2 if no local minima exist
  return $success if not @$rfLocalMinima;
 
  ## we start with the first element of local minima list
  push (@$rfDetected, shift(@$rfLocalMinima));
  while(defined($curr_lref = shift(@$rfLocalMinima))) {
    ## take the most recent detected local minima and call this "previous"
    $prev_dref = pop(@$rfDetected);
    ## at each iteration (thorugh local minima list) we take the "current"
    ## local minima and check if it overlaps with the previous local minima
    if($curr_lref->[0] <= ($prev_dref->[0] + $prev_dref->[1])) {
      ## if it does then we eleminate the one with larger s(t) value
      if($curr_lref->[1] < $prev_dref->[1]) {
        ## current local minima is greater than previous detected local minima
        ## so throw away current local minima and keep previous local minima
        push(@$rfDetected, $curr_lref);
      }
      else {
        ## current local minima is smaller than previous detected local minima
        ## so throw away previous local minima push current local minima init
        ## detected (non-overlapping) local minima list
        push(@$rfDetected, $prev_dref);
      }
      ## if non of the local (both previous and current) do not overlap then
      ## we keep (i.e., push) both local minima into detected local minima list
    }
    else {
      push(@$rfDetected, $prev_dref);
      push(@$rfDetected, $curr_lref);
    }
  }
 
  &debug($name, "Success") if $DEBUG_VERBOSE; 
  return $success;
} # end sub removeOverlap

## -------------------------------------------------------------------------- ##

## --| Bottom Searching |---------------------------------------------------- ##
sub bottomSearch {
## input:
##  arg 1: reference to list, representing collection times
##         (ref->ctimes[i] ==> ref->[ts, s(t)])
##  arg 2: reference to list, representing local minima after overlap removal
##         (ref->detected[i] ==> ref->[ts, s(t), index_into_ctimes])
##  arg 3: reference to list, representing local minima after bottom searching
##         (ref->bottomsearch[i] ==> ref->[ts, s(t)]
##  arg 4: int, bottom search interval (in seconds)
##  arg 5: int, bottom search start (in seconds)
##  arg 6: int, bottom search end   (in seconds)
##  arg 7: reference to list, representing local minima after enhanced bottom searching
##         (ref->bottomsearch[i] ==> ref->[ts, s(t)]
## output:
##  implicit return (through arg3) of bottom searched local minima
##  0 on success, positive integer on error

  ## function name used in error and debug messages
  my $name = "$progname:bottomSearch";
  my $success = 0;
  my $error = 1;
 
  ## 5 parameter required
  if (scalar @_ != 7) {
    &logError($name, "Expecting 7 arguments, but received (@_) instead");
    return $error;
  }
 
  my ($rfCtimes, $rfDetected, $rfBottomSearch, $bottomSearchInterval, $SE, $BE, $rfEndPoint) = @_;
  $SE = ($SE) ? $SE : 3; ##threshold for looking for START point
  $BE = ($BE) ? $BE : 3; ##threshold for looking for END   point
 
  ## For each local minima after overlap removal
  my %EndPoint = ();

  while(defined(my $dtd = shift(@$rfDetected))) {
    ## calculate for @rfBottomSearch
    my ($ts, $st, $i) = ($dtd->[0], $dtd->[1], $dtd->[2]);
    my $ribsize = $rfCtimes->[$i][2];

    ##--------------------------
    ## Bottom search (backward) 
    ## - for more accurate start time 
    ##--------------------------
    my $Diff_st_pre;  
    while($i-- && defined ($rfCtimes->[$i])) {
       my $srch_intr     = $ts - $rfCtimes->[$i][0]; # Difference between two updates' timestamp
       $Diff_st_pre = $rfCtimes->[$i][1]- $st;  # Difference between two collection times
       last if ($srch_intr > $bottomSearchInterval || $Diff_st_pre > $SE);
       $ts = $rfCtimes->[$i][0];
       $st = $rfCtimes->[$i][1];
    }
    push(@$rfBottomSearch, 
         [$rfCtimes->[$i+1][0], $rfCtimes->[$i+1][1]]
        );
    my $startPointTS = $rfCtimes->[$i+1][0];
    my $endPointTS_1 = $rfCtimes->[$i+1][0] + $rfCtimes->[$i+1][1] - 1; # [DEBUG #0]
    my $duration_1   = $rfCtimes->[$i+1][1];
    my $pre_ts       = $rfCtimes->[$i][0];
    my $pre_st       = $rfCtimes->[$i][1];
    ##--------------------------
    ## Bottom search (forward) 
    ## - for more accurate end time
    ##--------------------------
    ## calculate @rfEndPoint
    ($ts, $st, $i) = ($dtd->[0], $dtd->[1], $dtd->[2]);
    my $Diff_st_suc;  
    while ($i++ && defined ($rfCtimes->[$i])) {
       $Diff_st_suc = $rfCtimes->[$i][1] - $st;
       last if ($Diff_st_suc > $BE);
       $st = $rfCtimes->[$i][1];
    }
    my $endPointTS_2 = $rfCtimes->[$i-1][0] + $rfCtimes->[$i-1][1] - 1; # [DEBUG #0]
    my $duration_2   = $endPointTS_2 - $startPointTS + 1; # [DEBUG #0]
    my $suc_ts       = $rfCtimes->[$i][0];
    my $suc_st       = $rfCtimes->[$i][1];
 
    $EndPoint{$startPointTS} = [
                                #$startPointTS, $endPointTS_1, $duration_1, 
                                $startPointTS, $endPointTS_2, $duration_2, 
                                $pre_ts, $pre_st, $Diff_st_pre,
                                $suc_ts, $suc_st, $Diff_st_suc,
                                $ribsize,
                               ];
  }
  foreach my $ts (sort { $a <=> $b } keys %EndPoint)
  {
    push(@$rfEndPoint, $EndPoint{$ts});
  }

  &debug($name, "Success") if $DEBUG_VERBOSE;
  return $success;
} # end sub bottomSearch

## -------------------------------------------------------------------------- ##

## --| Helper Routines |----------------------------------------------------- ##
sub isValidDataStream {
## input:
##  arg 1: string, a line from text data stream (output of 'bgpdump -m')
##  arg 2: reference to hash, representing Field:Index pairs
## output:
##  int, 1 on success, 0 on validation failure
## Description:
##   check if input line belongs to a valid text data stream, i.e., if
##   input line is from 'bgpdump -m'

  ## function name used in error and debug messages
  my $name = "$progname:isValidDataStream";
  my $error = 0;
  my $success = 1;
 
  ## 2 parameters required
  if (scalar @_ != 2) {
    &logError($name, "Expecting 2 arguments, but received (@_) instead");
    return $error;
  }
 
  my ($line, $rfFieldIndex) = @_;
 
  ## validation RegExp
  my $rx_ipnum  = qr{([01]?\d\d?|2[0-4]\d|25[0-5])}; # quad of IP address
  my $rx_ipv4   = qr{^($rx_ipnum\.){3}$rx_ipnum$};   # ipv4 address
  my $rx_cidr_mask = qr{\d\d?};                      # CIDR prefix mask
  my $rx_ts     = qr{^\d+$};                         # time stamp
  my $rx_aws    = qr{^(A|B|W|STATE)$};               # A|B|W|STATE
  my $rx_ipv6   = qr{^((([0-9A-Fa-f]{1,4}:){7}[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){6}:[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){5}:([0-9A-Fa-f]{1,4}:)?[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){4}:([0-9A-Fa-f]{1,4}:){0,2}[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){3}:([0-9A-Fa-f]{1,4}:){0,3}[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){2}:([0-9A-Fa-f]{1,4}:){0,4}[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){6}((\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b)\.){3}(\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b))|(([0-9A-Fa-f]{1,4}:){0,5}:((\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b)\.){3}(\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b))|(::([0-9A-Fa-f]{1,4}:){0,5}((\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b)\.){3}(\b((25[0-5])|(1\d{2})|(2[0-4]\d)|(\d{1,2}))\b))|([0-9A-Fa-f]{1,4}::([0-9A-Fa-f]{1,4}:){0,5}[0-9A-Fa-f]{1,4})|(::([0-9A-Fa-f]{1,4}:){0,6}[0-9A-Fa-f]{1,4})|(([0-9A-Fa-f]{1,4}:){1,7}:))$};  # RFC 2373 ipv6 address

  my ($ts, $aws, $peer, $prefix) = (split('\|', $line))
                                   [ $$rfFieldIndex{'TS'},
                                     $$rfFieldIndex{'AWS'},
                                     $$rfFieldIndex{'PEER'},
                                     $$rfFieldIndex{'PREFIX'}
                                   ];
 
  if (!defined($ts) || !defined($aws) || !defined($peer) || !defined($prefix)) {
    &logError($name, "Unable to find required fields (TimeStamp, Msg. Type,".
       "Next Hop Peer, and Prefix) on input");
    return $error;
  }
 
  ## check if valid TimeStamp field
  if ($ts !~ m/$rx_ts/) {
    &logError($name, "Invalid time stamp field: $ts on data stream");
    return $error;
  }
 
  ## check if valid A|W|STATE field
  if ($aws !~ m/$rx_aws/) {
    &logError($name, "Invalid A|B|W|STATE field: $aws on data stream");
    return $error;
  }
 
  ## check if valid IP(v4, v6) field
  if ($peer !~ m/$rx_ipv4/) {
    if ($peer !~ m/$rx_ipv6/) {
      &logError($name, "Invalid IP field: $peer on data stream");
      return $error;
    }
  }
 
  ## STATE messages do not carry any prefix field
  return $success if ($aws eq 'STATE');
 
  ## check if valid CIDR (ipv6, ipv4) prefix
  my ($cidr_prefix, $cidr_mask) = split('\/', $prefix);
  if ($cidr_prefix !~ m/$rx_ipv4/) {
    if ($cidr_prefix !~ m/$rx_ipv6/) {
      &logError($name, "Invalid prefix field: $prefix on data stream");
      return $error;
    }
  }
  if ($cidr_mask !~ m/$rx_cidr_mask/) {
    &logError($name, "Invalid prefix field: $prefix on data stream");
    return $error;
  }
 
  &debug($name, "Success") if $DEBUG_VERBOSE;
  return $success;
} # end sub validateDataStream()

sub parseFieldIndex {
## input:
##  arg 1: string, containing comma seperated (special) field names and their
##                 corresponding field indices in the input stream. (Field
##                 indices starts from 1).
##  arg 2: string, containing comma seperated strings allowed as field names
## output: 
##  a hash containing keys with the same name as fields and their
##  corresponding indices.
##  (OR) undef on error;
## description:
##  construct a hash of "{Field} => Index" pairs by parsing Field:Index string

  ## function name used in error and debug messages
  my $name = "$progname:parseFieldIndex";
  my $error = undef;
 
  &debug($name, "called with (@_)") if $DEBUG_VERBOSE;
 
  ## 2 parameters required
  if (scalar @_ != 2) {
    &logError($name, "Expecting 2 argument, but received (@_) instead");
    return $error;
  }
 
  my ($fieldString, $validFieldIndex) = @_;
 
  ## hash: "{Field} => Index"
  my %FI;
 
  ## we only care about four fields: TS, A|W|STATE, PEER and PREFIX
  ## check if user passed in the proper format. 
  ## Format is "FIELD-1:index-1,FIELD-2:index-2,FIELD-3:index-3.."
 
  ## field string RegExp
  my $rx_fi_pair = qr{[A-Z]+:\d{1}};
  my $rx_fieldstring = qr{^($rx_fi_pair,){3}$rx_fi_pair$};
  
  $fieldString =~ s/\s*//; # strip whitespace
  if ($fieldString !~ m/$rx_fieldstring/) {
    &logError($name, "Invalid \"Field:Index\" string, please check ". "documentation for the appropriate format");
    return $error;
  }
  
  ## construct field => index hash
  foreach my $pair (split(',', $fieldString)) {
    my ($fieldName, $fieldIndex) = split(':', $pair);
    if ($validFieldIndex !~ m/$fieldName/) {
      &logError($name, "Invalid Field name, please check documentation ". "for the appropriate field names \n");
      return $error;
    }
    $FI{$fieldName} = $fieldIndex - 1; # perl indices start with 0
    &debug($name, "field name: $fieldName located at index: $FI{$fieldName} ". "on input") if $DEBUG_VERBOSE;
  }
 
  &debug($name, "Success") if $DEBUG_VERBOSE;
  return %FI;
} # end sub parseFieldIndex()

sub logError {
## input:
##  arg 1: string, representing module/subroutine name
##  arg 2: string, message to be printed on stderr
## output:
##  message printed to stderr
## description:
##  print log messages to stderr

  my $name = "logError";

  ## 2 parameters required
  if (scalar @_ != 2) {
    print STDERR ("$name Expecting 2 arguments, but received (@_) instead\n");
  }
 
  my ($functionName, $logMessage) = @_;
 
  ## strip out any unnecessary newline characters from log message
  ## we will add one later (when printing)
  chomp($logMessage);
 
  ## get current time values for use in log messages
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday) = localtime(time);
  $mon++; $year += 1900;
 
  print STDERR ("[$year.$mon.$mday $hour:$min:$sec] ", "[$name] ",
                "[$functionName] ", "[$logMessage]", "\n");
} # end sub logError()

sub debug {
## input:
##  arg 1: string, representing module/subroutine name
##  arg 2: string, message to printed on stdout
## output:
##  message printed to stdout
## description:
##  print debug messages to stdout

  my $name = "debug";
 
  ## 2 parameters required
  if (scalar @_ != 2) {
    print STDERR ("$name Expecting 2 arguments, but received (@_) instead\n");
  }
 
  my ($functionName, $debugMessage) = @_;
 
  ## strip out any unnecessary newline characters from debug message
  ## we will add one later (when printing)
  chomp($debugMessage);
 
  ## get current time values for use in log messages
  my ($sec, $min, $hour, $mday, $mon, $year, $wday, $yday) = localtime(time);
  $mon++; $year += 1900;
 
  print STDERR ("[$year.$mon.$mday $hour:$min:$sec] ", "[$name] ", "[$functionName] ", "[$debugMessage]", "\n");
} # end sub debug()

## -------------------------------------------------------------------------- ##

__END__

## --| Documentation |------------------------------------------------------- ##

=head1 NAME

B<bgpmct.pl> - BGP Minimum Collection Time Algorithm: Identify BGP Table
Transfers, given a stream of updates.

=head1 SYNOPSIS

bgpmct.pl [options] -p PeerIP -rf ribfile.txt.gz -ul updatefileslist.txt

=over 2

=item B<Ex>:

bgpmct.pl -rf ribfile.txt.gz -ul updatefiles.txt -p 1.2.3.4 

bgpmct.pl -t table size -ul updatefiles.txt -p 1.2.3.4

bgpmct.pl -rf ribfile.txt.gz -ul updatefiles.txt -p 1.2.3.4 -U 7200 -N 0.99
-B 10 -cf ctimes.txt -d

=back

=head1 DESCRIPTION

B<bgpmct.pl> uses the B<Minimum Collection Time (MCT)> algorithm to identify
BGP routing table transfers, given a stream of updates.

For each table transfer present in the input stream a pair [B<timestamp,
duration>] is output indicating I<when> (timestamp) a table transfer started and
for I<how long> (duration) it lasted.

Three parameters effect the operation of minimum collection time algorithm (MCT)

=over 3

=item 1. 

Maximum s(t) value (in seconds): B<U>

=item 2.

Expected Table Size Fraction (as percentage fraction): B<N>

=item 3.

Bottom Search Interval (in seconds): B<B>

=back

Please see B<REFERENCE> for a detailed explanation of these parameters and how
to set them appropriately (if you ever need to).

B<bgpmct.pl> expects both update files and ribfile in ASCII converted machine readable format as output by C<bgpdump -m> from I<libbgpdump> or C<route_btoa -m> from I<MRT>. Each input line should contain atleast one of B<A>nnounce, B<W>ithdraw, B<STATE>, or taB<B>le message type. Fields are separated by C<|> character. 

For example:

=over 3

=item A sample B<A>nnounce message:

BGP4MP|1009843632|A|12.127.0.121|7018|195.9.178.0/24|7018 1299 2118|INCOMPLETE|12.127.0.121|0|0||NAG||

=item A sample B<W>ithdraw message:

BGP4MP|1009843632|W|12.127.0.121|7018|202.54.24.0/24

=item A sample B<STATE> message:

BGP4MP|1010223783|STATE|12.127.0.121|7018|6|1

=item A sample taB<B>le entry:

TABLE_DUMP|1009870627|B|12.127.0.121|7018|3.0.0.0/8|7018 701 80|IGP|12.127.0.121|0|0||NAG||

=back

B<bgpmct.pl> only cares about B<Time Stamp> (I<field 2>), B<Message Type> (I<field 3>), B<Next Hop Peer> (I<field 3>), and B<Destination Prefix> (I<field 6>) for its operation. A special command line parameter B<--fieldindex> | B<-fi> allows the user to specify where each of these fields could be found in input.

Special care is required when specifying a ribfile (B<--ribfile> | B<-rf>). In the most general case, the ribfile should be from some time just before the first update file from B<--updatefileslist updatefiles.txt> list.

Update file(s) should cover atleast B<--range> | B<-U> seconds of updates for correct operation of the program. I.e., the difference between the first and last timestamp in update files should be atleast "B<U>" seconds. 

=head1 OPTIONS

B<bgpmct.pl> supports both long options (--option) and short options (-option).
Following is a detailed list of options (both long and short) supported by the
program.

=over 6

=item B<--ribfile ribfile.txt.gz>

=item B<-rf>

Gzip'd ASCII RIB file.

This RIB file should contain routes learned from peer specified via "-p peerIP" argument, any other routes learned from other peers are ignored.
Optional if "-t table size" is specified (i.e., pre-computed table size of peer).
Atleast one of "-rf ribfile.txt.gz" or "-t table size" is required by the program.

=item B<--ribsize positive integer>

=item B<-t>

BGP Table size of the peer specified via "-p" option.

Optional if "-rf ribfile.txt.gz" is specified (i.e., RIB file of peer specified
via "-p" option). Atleast one of "-rf ribfile.txt.gz" or "-t table size" is required by the program.

=item B<--updatefileslist updatefiles.txt>

=item B<-ul>

List of BGP update files from a monitoring project (Ex: RIPE, RouteViews).

Each line lists exactly one Gzip'd ASCII updates file. Note that the list may not be in chronological order but should not contain missing files (i.e., non-existing files).

=over 2

=item B<Ex:>

 /base/rv/oreg/2005.01/updates.20050101.0007.txt.gz
 /base/rv/oreg/2005.01/updates.20050101.0022.txt.gz
 /base/rv/oreg/2005.01/updates.20050101.0037.txt.gz
 .....

=back

=item B<--peer>

=item B<-p>

IP address of next hop peer of monitoring project's collection point.

Note that this peer should appear in the updates files and rib file (if any) supplied as input. 

=item B<--range positive integer>

=item B<-U>

Maximum s(t) value in seconds (Default: 7200 sec).

=item B<--percent float>

=item B<-N>

Expected table size fraction (Default: 0.99).

This fraction should be in the range [0, 1].

=item B<--bottomsearch positive integer>

=item B<-B>

Bottom searching threshold (Default: 10 sec).

=item B<--ctimefile ctimes.peer.txt>

=item B<-cf>

Write collection times to file 'ctimes.peer.txt'.

=item B<--fieldindex>

=item B<-fi>

A string of comma separated B<Field:Index> pairs.

Each I<Field:Index> pair denotes where a particular I<field> can be found in input stream. Fields can only be one of these: B<TS>, B<AWS>, B<PREFIX>, B<PEER> and B<*all*> fields are mandatory. (Default: "TS:2,AWS:3,PEER:4,PREFIX:6")

=item B<--debug [a|cdov]>

=item B<-d>

Enable debug and verbose output like collection times, local minima list before
and after overlap removal .. etc.

The optional "a (or) cdov" fields have the following meaning:

=over 4 

=item B<a>

Enable printing ALL debug information. You can achieve the same effect by
specifying "--debug" or "-d" by itself (without the optional fields).

=item B<c>

Enable printing collection time results.

=over 2

=item I<Note>:

You can always save the collection time results by specifying "-c ctime.txt"
on command line.

=back

=item B<d>

Enable printing all local minima list (i.e., before overlap removal)

=item B<o>

Enable printing local minima list after overlap removal

=item B<v>

Enable printing verbose information at each stage of program execution.

=back

=item B<--validate>

Normally input stream is passed through certain validation checks to determine
if user supplied correct input. Use this option (without arguments) in case
you wish to validate the entire input stream (at a B<HUGE> additional processing
expense).

=item B<--help>

=item B<-h>

Print help, (this message).

=back

=head1 AUTHORS

    Vamsi K. Kambhampati (current maintainer)
    vamsi@cs.colostate.edu
    Colorado State University, Fort Collins.

Please contact the current maintainer (vamsi@cs.colostate.edu) for reporting
bugs and improvements. Thanks!

=head1 REFERENCE

"Identifying BGP Routing Table Transfer", Beichuan Zhang, Vamsi Kambhampati, Mohit Lad, Daniel Massey, and Lixia Zhang. ACM SIGCOMM MineNet Workshop, Aug 2005.

=cut

# vim: sw=2 sts=2 st=2 expandtab 
