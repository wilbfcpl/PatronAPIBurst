# Author:  <wblake@CB95043>
# Created: Feb 26, 2025
# Version: 0.01
#
# Usage: perl [-g] [-r ] [-x]fcps_update.pl filename.csv-
# -g Debug/verbose -r read only , no update, -x delete for cleanup of test data
# 
# filename.csv is a file with FCPS Student information for their FCPL Student Success Card Account
# patronid, firstName, middleName, lastName,grade, schoolAddress ,city , state , zip, enrollmentStatus
#
#If the student's account already exists, update it. Otherwise create it.
#
#Read only mode, no updates- check if the student exists. Delete mode - for removing test data.
#Debug mode- a lot more SOAP messages.
#
# Assumes first line of in file has column label headings
#
# Uses local copy of CarlX WSDL file PatronAPI.wsdl for interface to PatronAPI requests GetPatronInformation, UpdatePatron,CreatePatron,DeletePatron
#
# A tool like SOAPUI can provide a sandbox for the WSDL file and PatronAPI requests.
#
# Note that PatronAPI.wsdl may violate rules by responding to CreatePatron and UpdatePatron.
# Note that API call and response return appear to take one second in real time.
#
# An SQL Query to select updated Student Patron records
# select patronid, name, bty, regdate from patron_v2 where bty=10 and regdate>'20-JAN-22';
#

use strict;
use diagnostics;
#use integer;

use File::Spec;
use Parallel::ForkManager;
use Data::Dumper;
use feature 'say';
use Getopt::Std;
use Log::Log4perl qw(:easy);

# For error handling
use Try::Tiny;


#$TRACE,$DEBUG,$INFO,$WARN,$ERROR,$FATAL
Log::Log4perl->easy_init($ERROR);

# See the CPAN and web pages for XML::Compile::WSDL
# http://perl.overmeer.net/xml-compile/
use XML::Compile::WSDL11;	# use WSDL version 1.1
use XML::Compile::SOAP11;	# use SOAP version 1.1
use XML::Compile::Transport::SOAPHTTP;

# Reduce number of magic values where possible
use constant API_CHUNK_SIZE => 16;
use constant LAST_EDITED_API => 'API';
use constant SEARCHTYPE_PATRONID => 'Patron ID';

# Chunking
my $nr;
my $num_chunks ;
my $mods ;

# Results and trace from XML::Compile::WSDL et al.
my %trace;
my %results;
# Timestamp for the Patron field PATRON_V2. REGDATE 
#my $edittime;
my $todaydate;

#PatronAPI WSDL Call vars
my $call2;


# API Call Hash Vars
# PatronAPI Request Vars
my %PatronUpdateValues;
my %PatronUpdateRequest;
my $MyResponseStatusCode;

# Some counting variables to gauge the damage done
my $UpdatedPatrons = 0;


# input file fields, patron record fields

my ($patronid, $first, $middle, $last,$grade, $address ,$city , $state , $zip, $status);

#Arrays holding values for simultaneously forked requests
my (@patronid, @first, @middle, @last,@grade, @address ,@city , @state , @zip, @status,@edittime);


my $pid;
my (@result,@trace);
my (@result2,@trace2);

#For Parallel::ForkManager
my $pm;

#Command line input variable handling. $opt_g debug, $opt_x delete recs, $opt_q q quiet, $opt_r r readonly

our ($opt_g,$opt_x,$opt_q, $opt_r,%opts);
getopts('gx:qr');

my $local_filename=$0;
$local_filename =~ s/.+\\([A-z]+.pl)/$1/;
       
use if defined $opt_g, "Log::Report", mode=>'INFO';

    
# Use today if date option not provided
my ($day,$month,$year) = (localtime) [3,4,5];
$year+= 1900;
$month += 1;

$todaydate = "$year-$month-$day";
             
#See CPAN, web pages for XML::Compile::WSDL
# http://perl.overmeer.net/xml-compile/

my $wsdlfile = 'PatronAPInew.wsdl';

sub setupWSDL
{
my $wsdl;
try {
    TRACE "[$local_filename:" . __LINE__ . "] Loading WSDL from $wsdlfile";
    $wsdl = XML::Compile::WSDL11->new($wsdlfile);
    unless (defined $wsdl) {
    FATAL "[$local_filename:" . __LINE__ . "] Failed XML::Compile call - WSDL object not created";
    die "Failed XML::Compile call - WSDL object not created";
    }
    
    TRACE "[$local_filename:" . __LINE__ . "] Compiling WSDL clients";
    $call2 = $wsdl->compileClient('UpdatePatron');
    
    unless (defined $call2) {
    FATAL "[$local_filename:" . __LINE__ . "] SOAP/WSDL Error - Failed to compile clients";
    die "SOAP/WSDL Error - Failed to compile clients";
    }
    
    INFO "[$local_filename:" . __LINE__ . "] WSDL setup completed successfully";
} catch {
    FATAL "[$local_filename:" . __LINE__ . "] WSDL Setup Error: $_";
    die "WSDL Setup Error: $_";
};
}
# These values will remain the Same for all the PatronAPI invocations

# Others will change for each loop invocation / Patron ID in the input file

sub setupAPIConstantVals
{
  %PatronUpdateRequest =
    (
      SearchType => SEARCHTYPE_PATRONID,
      Patron => \%PatronUpdateValues,
      Modifiers=> {
	DebugMode=>1,
	ReportMode=>1}
    );
# use LAST_EDITED_API for now, something else later
  %PatronUpdateValues =
      (
     RegisteredBy => LAST_EDITED_API
    );
}

# Buffer size a global manifest constant.
# Determine number of bursts by dividing the number of lines in the file
# by the burst size.

sub setupChunking
{
    
  unless ( defined $ARGV[0])  {
    die "[$local_filename" . ":" . __LINE__ . "Usage: perl $local_filename  [-gxqr] <input file>\n";
 }
    
  my $filename = $ARGV[0];
  open(my $fh, '<', $filename) or die "Could not open file '$filename' $!";

 $nr  = 0;
 while (my $line = <$fh>) {
    $nr++;
 }

close($fh);

$num_chunks = ($nr-1)/API_CHUNK_SIZE;
 $mods = ( ($nr-1) %API_CHUNK_SIZE);

  
  WARN "[$local_filename" . ":" . __LINE__ . "]Linecount " . $nr . " Burst Size " . API_CHUNK_SIZE . " Bursts " . $num_chunks . " Mod " . $mods;

}
sub setPatronParams
{
  my ($current_line) = @_;
  
  # PatronAPI Search param

  $PatronUpdateRequest{SearchID} = $patronid[$current_line];
  $PatronUpdateValues{LastEditDate}=$edittime[$current_line];
	
}

sub setupFork {
try {
    INFO "[$local_filename:" . __LINE__ . "] Setting up Parallel::ForkManager";
    $pm = Parallel::ForkManager->new(
        max_proc => API_CHUNK_SIZE,
        tempdir => File::Spec->tmpdir()
    );
    
    # Protect against memory leaks by ensuring cleanup of temporary files
    $pm->set_max_procs(API_CHUNK_SIZE);
    
    # Handle child process errors
    $pm->run_on_start(sub {
    my ($pid, $ident) = @_;
    #INFO "[$local_filename:" . __LINE__ . "] Started child process $pid";
    });
    
    $pm->run_on_finish(sub {
    my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure_reference) = @_;
    
    # Log process completion with detailed information
    INFO "[$local_filename:" . __LINE__ . "] Child $pid completed with exit_code: $exit_code, signal: " . 
        ($exit_signal || 'none') . ", core_dump: " . ($core_dump || 'none');
    
    
    if (defined ($data_structure_reference)) {
        my $entry = $data_structure_reference->{entry_line};        
        $UpdatedPatrons += $data_structure_reference->{updatedPatrons} if defined($data_structure_reference->{updatedPatrons});
        $results{$entry} = ${$data_structure_reference->{result}};
    } else {
        WARN "[$local_filename:" . __LINE__ . "] Child $pid completed without returning data";
    }
    });
    
    # Handle errors in child processes
    $pm->run_on_wait(sub {
    # Log every few seconds while waiting for children
    my $last_report_time = 0;
    my $now = time();
    if ($now - $last_report_time >= 10) {  # report every 10 seconds
        $last_report_time = $now;
        INFO "[$local_filename:" . __LINE__ . "] Waiting on " . $pm->running_procs . " child processes";
        
        # Memory tracking removed
    }
    }, 0.1);  # Check every 0.1 seconds
    
    INFO "[$local_filename:" . __LINE__ . "] Parallel::ForkManager setup completed";
} catch {
    FATAL "[$local_filename:" . __LINE__ . "] Error in setupFork: $_";
    die "Error in setupFork: $_";
};
}

sub getCreateUpdateAPIResponse
{ 
my ($result) = @_;

try {
    if (!defined($result) || !defined($result->{GenericResponse})) {
    WARN "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] Invalid response result structure";
    return undef;
    }
    
    if (!defined($result->{GenericResponse}->{ResponseStatuses}) || 
        !defined($result->{GenericResponse}->{ResponseStatuses}->{cho_ResponseStatus})) {
    WARN "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] Missing ResponseStatuses in API response";
    return undef;
    }
    
    my $code = undef;
    eval {
    $code = $result->{GenericResponse}->{ResponseStatuses}->{cho_ResponseStatus}[0]->{ResponseStatus}->{Code};
    };
    
    if ($@) {
    WARN "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] Error accessing response code: $@";
    return undef;
    }
    
    if (defined($code)) {
    INFO "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] UpdatePatron Response Status $code";
    return $code;
    } else {
    WARN "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] Undefined response code in API response";
    return undef;
    }
} catch {
    ERROR "[$local_filename:getCreateUpdateAPIResponse:" . __LINE__ . "] Exception processing API response: $_";
    return undef;
};
}

setupWSDL;
setupChunking;
setupAPIConstantVals;
setupFork;

if (defined ($opt_r)) {
  INFO "[$local_filename" . ":" . __LINE__ . "]Read Only: $opt_r " ;
}

# Read the input file and ignore the first line having column headings
$_ = <>;
chomp;

INFO "[$local_filename" . ":" . __LINE__ . "]Read Header Row Record $_";

my $burst_len;

# Break requests into API_CHUNK_SIZE blocks
for (my $current_block=0; $current_block<=$num_chunks;$current_block++ ) {
   
    
  INFO "[$local_filename" . ":" . __LINE__ . "]Burst $current_block of $num_chunks";

  #loop read the Patron values from the input file into the buffers
  for (my $current_line=0; defined($_) && $current_line<API_CHUNK_SIZE;$current_line++) {

    $_=<>;
        
    if (!defined($_)) {
      last;
    } elsif (($_ =~/^ *$/) || ($_ =~/^\s*$/)) {
      next;
    } else {
      chomp ;
	 
      ($patronid[$current_line], $first[$current_line], $middle[$current_line], $last[$current_line],$grade[$current_line], $address[$current_line],$city[$current_line] , $state[$current_line] , $zip[$current_line], $status[$current_line],$edittime[$current_line]) = split(/,/);	  
      INFO "[$local_filename" . ":" . __LINE__ . "]Input Line " . ($current_block * API_CHUNK_SIZE + $current_line + 1 . " $_");
    }

    # Burst len 
    $burst_len = $current_line + 1 ;
  }
   
  #send PatronAPI calls in API_CHUNK_SIZE bursts via Parallel::Forkmanager
  for (my $current_line=0;  $current_line<$burst_len; $current_line++) {

    my $updatedPatrons;
		 
    INFO "[$local_filename" . ":" . __LINE__ . "]Find $patronid[$current_line] burst " . $current_block. " line " . ($current_block * API_CHUNK_SIZE + $current_line + 1) . " Burst Size " . $burst_len;
    # Start fork with error handling
    # Use a more conventional control flow structure to avoid 'next' exiting subroutine/eval
    my $continue_with_child = 1; # Flag to determine if we should continue with child code
    try {
        $pid = $pm->start;
        if ($pid) {
            INFO "[$local_filename:" . __LINE__ . "] Started child process $pid for patron ID: $patronid[$current_line]";
            $continue_with_child = 0; # Parent process - skip child code
        }
    } catch {
        ERROR "[$local_filename:" . __LINE__ . "] Failed to fork proc: $_ burst $current_block line $current_line";
        $continue_with_child = 0; # Error occurred - skip child code
    };

    # Only continue with child process code if flag is set
    if ($continue_with_child) {

    #Patron Values for update
	
    setPatronParams($current_line);
 	    
    #Patron Assumed to exist so will update unless read-only
      if (!defined($opt_r)) {   
	  
	#Update the Patron Record

	  try {
	      DEBUG "[$local_filename:" . __LINE__ . "] Calling UpdatePatron API for PatronID: $patronid[$current_line]";
	      ($result2[$current_line], $trace2[$current_line]) = $call2->(%PatronUpdateRequest);

	      INFO "[$local_filename:" . __LINE__ . "] Burst $current_block proc $current_line API returned";

	      # Response status
	      if (!defined $result2[$current_line]) {
		  ERROR "[$local_filename:" . __LINE__ . "] No response from UpdatePatron API call for PatronID: $patronid[$current_line]";
	      } else {
		  my $status_code = getCreateUpdateAPIResponse($result2[$current_line]);
		  if (!defined $status_code) {
		      WARN "[$local_filename:" . __LINE__ . "] Unable to get status code from UpdatePatron API for PatronID: $patronid[$current_line]";
		  }
	      }
	  }
	  catch {
	      ERROR "[$local_filename:" . __LINE__ . "] Exception in UpdatePatron API call for PatronID $patronid[$current_line]: $_ ";
};
	  $updatedPatrons += 1;
	  INFO "[$local_filename" . ":" . __LINE__ . "]Update Student PatronID $patronid[$current_line] First $first[$current_line] Edit Time $edittime[$current_line]" ;
 }    
      try {
	  $pm->finish(0, {entry_line=>$current_line, result=>\$MyResponseStatusCode, updatedPatrons=>$updatedPatrons, foundPatrons=>0});
    } catch {
        ERROR "[$local_filename:" . __LINE__ . "] Error in finish for patron $patronid[$current_line]: $_ burst $current_block line $current_line";
        $pm->finish(1); # Finish with error code if data passing fails
    };
	    
    } # Close the if ($continue_with_child) block

    } # end for (my $current_line=0; $current_line<API_CHUNK_SIZE; $current_line++)
try {
    INFO "[$local_filename:" . __LINE__ . "] Waiting for all child processes to complete";
    $pm->wait_all_children;
    INFO "[$local_filename:" . __LINE__ . "] All child processes completed successfully";
    INFO "[$local_filename" . ":" . __LINE__ . "]Chunk Results: " . Dumper \%results ;
} catch {
    ERROR "[$local_filename:" . __LINE__ . "] Error waiting for child processes: $_ Block $current_block";
}

} # end for (my $current_block=0; $current_block<$num_chunks;$current_block++ )
WARN "[$local_filename" . ":" . __LINE__ . "]Updated $UpdatedPatrons" ;
