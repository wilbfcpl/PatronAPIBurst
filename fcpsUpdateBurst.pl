use strict;
use diagnostics;
use integer;
use Time::HiRes qw( gettimeofday tv_interval);
use Parallel::ForkManager;
use Data::Dumper;
use say;
use Getopt::Std;
use Log::Log4perl qw(:easy);

#TRACE,DEBUG,INFO,WARN,ERROR,FATAL
Log::Log4perl->easy_init($TRACE);

# See the CPAN and web pages for XML::Compile::WSDL
# http://perl.overmeer.net/xml-compile/
use XML::Compile::WSDL11;	# use WSDL version 1.1
use XML::Compile::SOAP11;	# use SOAP version 1.1
use XML::Compile::Transport::SOAPHTTP;

use constant API_CHUNK_SIZE => 8;
# Reduce number of magic values where possible
use constant PATRONTYPE_STUDENT => "STUDNT";
use constant STUDENT_BRANCH => "SSL";
use constant PATRONTYPE_WEBREG => "WEBREG";
use constant LAST_EDITED_API => 'API';
use constant SEARCHTYPE_PATRONID => 'Patron ID';
use constant MARYLAND => 'MD';
use constant ADDRESS_TYPE_PRIMARY =>'Primary';
use constant SOFTBLOCK_NOTE_TEXT => \
  'We would like to upgrade your library card. Please contact an FCPL staff member at your local branch or via email.' ;

use constant SOFTBLOCK_NOTE_TYPE => 2;
#Time::HiRes qw( gettimeofday tv_interval) related variables
# my $t0;
# my $elapsed;

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
my $call1;
my $call2;
my $call3;
my $call4;
my $call5;



# API Call Hash Vars
# PatronAPI Request Vars
my %PatronRequest ;
my %PatronCreateRequest ;
my %PatronUpdateValues;
my %PatronUpdateRequest;
my $MyResponseStatusCode;
    
# PatronAPI Response vars for GetPatronInformation. 
my $responsePatronID ;
my $responseFullName ;
my $responseDefaultBranch ; 
my $responsePatronStatusCode ;
my $responseRegisteredBy ;
my $responseRegistrationDate ; 
my $responseExpirationDate ;
my $responseUDFGradeValue ; 
my $responseUDFNewsletter  ;
my $responseUDFPreferredLang ; 
my $responseAddress ;

# Some counting variables to gauge the damage done
my $NewStudents = 0;
my $UpdatedStudents = 0;
my $FoundStudents = 0;
my $NotFoundStudents = 0;
my $deletedStudents = 0;

# input file fields, patron record fields

my ($patronid, $first, $middle, $last,$grade, $address ,$city , $state , $zip, $status);

#Arrays holding values for simultaneously forked requests
my (@patronid, @first, @middle, @last,@grade, @address ,@city , @state , @zip, @status,@edittime);


my $pid;
my (@result,@trace);
my (@result2,@trace2);
my (@result3,@trace3);

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
  my $wsdl = XML::Compile::WSDL11->new($wsdlfile);
  unless (defined $wsdl) {
    die "[$local_filename" . ":" . __LINE__ . "]Failed XML::Compile call\n" ;
  }

  $call1 = $wsdl->compileClient('GetPatronInformation');
  $call2 = $wsdl->compileClient('UpdatePatron');
  $call3 = $wsdl->compileClient('CreatePatron');
  $call4 = $wsdl->compileClient('DeletePatron');
  $call5 = $wsdl->compileClient('AddPatronNote');
  unless ((defined $call1) && (defined $call2) && (defined $call3) && ( defined $call4) ) {
    die "[$local_filename" . ":" . __LINE__ . "] SOAP/WSDL Error $wsdl $call1, $call2 $call3 $call4 \n" ;
  }
}
# These values will remain the Same for all the PatronAPI invocations

# Others will change for each loop invocation / Patron ID in the input file

sub setupAPIConstantVals
{  
  %PatronRequest =
    (
      SearchType => SEARCHTYPE_PATRONID,
      Modifiers=> { DebugMode=>1,
		    ReportMode=>1,}
    );
     
  %PatronUpdateRequest =
    (
      SearchType => SEARCHTYPE_PATRONID,
      Patron => \%PatronUpdateValues,
      Modifiers=> {
	DebugMode=>1,
	ReportMode=>1}
    );

  %PatronCreateRequest =
    (
      Patron => \%PatronUpdateValues,
      Modifiers=> {
	DebugMode=>1,
	ReportMode=>1}
    );
    

  %PatronUpdateValues =
    (
      PatronType=> PATRONTYPE_STUDENT,
      PreferredBranch=>STUDENT_BRANCH,
      RegBranch=>STUDENT_BRANCH,
      DefaultBranch=>STUDENT_BRANCH,
      RegisteredBy => LAST_EDITED_API,
      UserDefinedFields=>
      {
	cho_UserDefinedField=>
	{
	  UserDefinedField=>
	  {
	    Field=>'Grade'
	  }
	}
      },
      #      RegistrationDate=>$edittime,
      Addresses => { cho_Address=>
		     {Address =>
		      {
			Type =>ADDRESS_TYPE_PRIMARY,
			State=>MARYLAND
		      }
		    }
		   }
    );
}

# Buffer size a global manifest constant.
# Determine number of bursts by dividing the number of lines in the file
# by the burst size.


sub setupChunking
{
  # Use wc -l to get line count of input file $ARGV[0]
  $nr = qx/wc -l $ARGV[0]/ ;
  if ($? != 0) {
    die "[$local_filename" . ":" . __LINE__ . "]shell returned $?";
  }
  
  $nr =~ s/^([0-9]+).*/$1/;
  chomp($nr);
  #[$local_filename" . ":" . __LINE__ . "]DBI Call lapsed time $elapsed."
  $num_chunks = $nr/API_CHUNK_SIZE;
  $mods = ($nr%API_CHUNK_SIZE) - 1;


  
  WARN "[$local_filename" . ":" . __LINE__ . "]Linecount " . $nr . " Burst Size " . API_CHUNK_SIZE . " Bursts " . $num_chunks . " Mod " . $mods;

}
sub setPatronParams
{
  my ($current_line) = @_;
  
  # PatronAPI Search param
  $PatronRequest{SearchID} = $patronid[$current_line];
  $PatronUpdateRequest{SearchID} = $patronid[$current_line];
	
  #Patron Values for update or creation
  $PatronUpdateValues{PatronID}=$patronid[$current_line];
  $PatronUpdateValues{FirstName} =   $first[$current_line];
  $PatronUpdateValues{MiddleName} = $middle[$current_line];
  $PatronUpdateValues{LastName}   = $last[$current_line];
  $PatronUpdateValues{UserDefinedFields}{cho_UserDefinedField}{UserDefinedField}{Value}=$grade[$current_line];
  $PatronUpdateValues{Addresses}{cho_Address}{Address}{Street}=$address[$current_line];
  $PatronUpdateValues{Addresses}{cho_Address}{Address}{City}=$city[$current_line];
  $PatronUpdateValues{Addresses}{cho_Address}{Address}{PostalCode}=$zip[$current_line];
  $PatronUpdateValues{PatronStatusCode} = $status[$current_line];
  $PatronUpdateValues{RegistrationDate}=$edittime[$current_line];
}

sub setupFork{
  $pm =  Parallel::ForkManager->new(
    max_proc=>API_CHUNK_SIZE,
    tempdir =>'/tmp'
  );
  
  $pm->run_on_finish( sub {
			my ($pid, $exit_code, $ident, $exit_signal, $core_dump, $data_structure_reference) = @_;
			if (defined ($data_structure_reference)) {
			  my $entry=$data_structure_reference->{entry_line};
     
			  $FoundStudents += $data_structure_reference->{foundStudents} if defined( $data_structure_reference->{foundStudents});
			  $NotFoundStudents += $data_structure_reference->{notfoundStudents} if defined( $data_structure_reference->{notfoundStudents});
			  $NewStudents += $data_structure_reference->{newStudents} if defined( $data_structure_reference->{newStudents});
			  $UpdatedStudents += $data_structure_reference->{updatedStudents} if defined( $data_structure_reference->{updatedStudents});
			  $results{$entry} =  ${$data_structure_reference->{result}};
			}
		      });
}

sub getCreateUpdateAPIResponse
{ 
  my ($result) = @_;

  my $code=($result->{GenericResponse}->{ResponseStatuses}->{cho_ResponseStatus}[0]->{ResponseStatus}->{Code});

  if (defined($code)) {
    INFO "[$local_filename" . ":getCreateUpdateAPIResponse:" . __LINE__ . "]UpdatePatron Response Status $code" ;
  }
     
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
   
    
  INFO "[$local_filename" . ":" . __LINE__ . "]Burst $current_block";

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
      INFO "[$local_filename" . ":" . __LINE__ . "]Buffering Current line " . ($current_block * API_CHUNK_SIZE + $current_line + 1 . " $_");
    }

    # Burst len 
    $burst_len = $current_line + 1 ;
  }
   
   
  #send PatronAPI calls in API_CHUNK_SIZE bursts via Parallel::Forkmanager
  for (my $current_line=0;  $current_line<$burst_len; $current_line++) {
    my $foundStudents;
    my $updatedStudents;
    my $newStudents;
    my $notFoundStudents;
		 
    INFO "[$local_filename" . ":" . __LINE__ . "]Find $patronid[$current_line] Current line " . ($current_block * API_CHUNK_SIZE + $current_line + 1) . " Burst Len " . $burst_len;
    $pid = $pm->start and next;

	
    #Find existing student rec to update, otherwise create a new one
    #Patron Values for update or creation
	
    setPatronParams($current_line);
 	
    # Find Patron if they already exist
    ($result[$current_line], $trace[$current_line]) = $call1->(%PatronRequest);

    #[TODO] API Response Handler
    $MyResponseStatusCode = ($result[$current_line]->{GetPatronInformationResponse}->{ResponseStatuses}->{cho_ResponseStatus}[0]->{ResponseStatus}->{Code});

    #Found the Patron so will update unless read-only
    if ( (defined $MyResponseStatusCode) && ($MyResponseStatusCode == 0) ) {
      INFO "[$local_filename" . ":" . __LINE__ . "]Found. GetPatronInfo Response Status $MyResponseStatusCode" ;
      $foundStudents += 1;

      if (!defined($opt_r)) {   
	  
	#Update the Patron Record

	($result2[$current_line],$trace2[$current_line]) = $call2->(%PatronUpdateRequest);

	INFO "[$local_filename" . ":" . __LINE__ . "]Burst $current_block proc $current_line API returned";

	# Response status
	getCreateUpdateAPIResponse($result2[$current_line]);

	$updatedStudents += 1;
	INFO "[$local_filename" . ":" . __LINE__ . "]Update Student PatronID $patronid[$current_line] First $first[$current_line] Edit Time $edittime[$current_line]" ;
      }
      $pm->finish (0,{entry_line=>$current_line,result=>\$MyResponseStatusCode,updatedStudents=>$updatedStudents,foundStudents=>$foundStudents});
    }
	
    # Did not find the Patron, so create a new account with the information
    else {
	 
      $notFoundStudents += 1;
	  
      INFO "[$local_filename" . ":" . __LINE__ . "]Not Found.  GetPatronInfo Response Status $MyResponseStatusCode" ;

	  
      if (!defined($opt_r)) {
	($result3[$current_line],$trace3[$current_line]) = $call3->(%PatronCreateRequest);

	getCreateUpdateAPIResponse($result3[$current_line]);
	  	  
	$newStudents += 1;
	INFO "[$local_filename" . ":" . __LINE__ . "]Create Student PatronID $patronid[$current_line] First $first[$current_line] Edit Time $edittime[$current_line]" ;
      }
      $pm->finish (0,{entry_line=>$current_line,result=>\$MyResponseStatusCode,newStudents=>$newStudents,notFoundStudents=>$notFoundStudents});       
    }


  } # end for (my $current_line=0; $current_line<API_CHUNK_SIZE; $current_line++)
  $pm->wait_all_children;

  INFO "[$local_filename" . ":" . __LINE__ . "]Chunk Results: " . Dumper \%results ;
    
} # end for (my $current_block=0; $current_block<$num_chunks;$current_block++ )

WARN "[$local_filename" . ":" . __LINE__ . "]Found $FoundStudents Not Found $NotFoundStudents New $NewStudents Updated $UpdatedStudents" ;

