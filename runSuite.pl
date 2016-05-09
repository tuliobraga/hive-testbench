#!/usr/bin/perl

use strict;
use warnings;
use File::Basename;
use POSIX qw(strftime);

# PROTOTYPES
sub dieWithUsage(;$);

# GLOBALS
my $SCRIPT_NAME = basename( __FILE__ );
my $SCRIPT_PATH = dirname( __FILE__ );

# MAIN
dieWithUsage("one or more parameters not defined") unless @ARGV >= 1;
my $suite = shift; # suite tpcds or tpch
my $scale = shift || 2; # data scale factor
my $iter = shift || 1; # number of executions of each query
my $interval = shift || 0; #interval between each query
my $queryName = shift || "*"; #query name ex.: query12
dieWithUsage("suite name required") unless $suite eq "tpcds" or $suite eq "tpch";

chdir $SCRIPT_PATH;
if( $suite eq 'tpcds' ) {
	chdir "sample-queries-tpcds";
} else {
	chdir 'sample-queries-tpch';
} # end if
my @queries = glob "$queryName.sql";

my $db = { 
	'tpcds' => "tpcds_bin_partitioned_orc_$scale",
	'tpch' => "tpch_flat_orc_$scale"
};

print "filename,status,start,end,time(s),avgtime(s),timeTaken(s),avgTimeTaken(s),standardDeviation\n";
$| = 1;
for my $query ( @queries ) {
	# declaring aux variables
	my $sumSquareTimeTaken = 0;
	my $sumTimeTaken = 0;
	my $variance = 0;
	my $standardDeviation = 0;
	my $rows = 0;

	# getting start datetime
	my $hiveStartDate = strftime "%F %H:%M:%S", localtime;
	my $hiveStart = time();

	for (my $i=0; $i < $iter; $i++) {
		my $logname = "$query.tez.$i.log";
		my $cmd="echo 'use $db->{${suite}}; source $query;' | hive -i testbench.settings 2>&1  | tee logs/$logname";
		my @hiveoutput=`$cmd`;
		die "${SCRIPT_NAME}:: ERROR:  hive command unexpectedly exited \$? = '$?', \$! = '$!'" if $?;

		# Time taken running the query based on log
		my $timeTaken = 0;
		my $avgTimeTaken = 0;

		my $repetitionNumber = 0;
		foreach my $line ( @hiveoutput ) {
			if( $line =~ /^Time taken:\s+([\d\.]+)\s+seconds/ ) {

				$timeTaken += $1;

			} elsif( $line =~ /^FAILED: / ) {
				my $hiveFail = time();
				my $hiveFailTime = $hiveFail - $hiveStart;
				print "$query,failed,$hiveFailTime\n"; 
			} # end if
		} # end while

		$sumSquareTimeTaken += $timeTaken*$timeTaken;
		$sumTimeTaken += $timeTaken;
	}

	my $hiveEnd = time();
	my $hiveTime = $hiveEnd - $hiveStart;

	# getting start datetime
	my $hiveEndDate = strftime "%F %H:%M:%S", localtime;

	# calculating time average considering number of repetitions
	my $hiveAvgTime = $hiveTime/$iter;

	if ($iter > 1) {
		$variance = ($sumSquareTimeTaken - ($sumTimeTaken*$sumTimeTaken)/$iter)/($iter-1);
		$standardDeviation = sqrt($variance);
	}

	my $avgTimeTaken = $sumTimeTaken/$iter;

	print "$query,success,$hiveStartDate,$hiveEndDate,$hiveTime,$hiveAvgTime,$sumTimeTaken,$avgTimeTaken,$standardDeviation\n"; 

	sleep($interval);
} # end for


sub dieWithUsage(;$) {
	my $err = shift || '';
	if( $err ne '' ) {
		chomp $err;
		$err = "ERROR: $err\n\n";
	} # end if

	print STDERR <<USAGE;
${err}Usage:
	perl ${SCRIPT_NAME} [tpcds|tpch] [scale]

Description:
	This script runs the sample queries and outputs a CSV file of the time it took each query to run.  Also, all hive output is kept as a log file named 'queryXX.sql.log' for each query file of the form 'queryXX.sql'. Defaults to scale of 2.
USAGE
	exit 1;
}