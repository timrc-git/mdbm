#!/usr/bin/perl -w

# Copyright 2013 Yahoo! Inc.                                         #
# See LICENSE in the root of the distribution for licensing details. #

# Run a performance test to record speed improvement building a large db.
#
# Has 2 main functions:
# 1. Take the output of the MDBM (currently only unit) tests and output TAP files that list
#    performance timings of each test.

use File::Basename;
use FileHandle;
use DirHandle;
use Getopt::Long;
use strict;

my $InputFile;
my $BaselineResults;
my $FilePrefix = "";
my $PercentRange;
my $UpperPerfRange = 1.05;   # Default to allow 5% worse performance

# Directory used to store TAP files resulting from comparison against the baseline results.
#  If doesn't exist, create and populate it with the resulting TAP files.
my $TapDirectory;

my $CompareDirectory;  # Directory containing new performance data to compare against baseline

sub usage
{
    my $script = basename($0);
    print STDERR ($script . " [options] 

Options:

--test-output filename   File containing the redirected output of the tests
--baseline-directory     Path to a file containing baseline performance results to compare against
--results-directory      Directory to store the performance-result TAP files created by the script
--other-directory        Directory containing TAP files to compare against the baseline results
--percent-range number   This number will be used as a percent of the allowed
                         performance range. ex: --percent-range 6 means 6% above
                         current benchmark. So if a current benchmark is 100, then
                         a valid performance time would be <= 106
-h, --help               Usage of this script

Typical use case 1 - How to generate TAP files by parsing output of tests:

    perf_compare.pl --test-output /tmp/make_test.out --results-directory /tmp/current_results

Typical use case 2 - How to compare results against a baseline:

    perf_compare.pl --baseline-directory /tmp/baseline --other-directory /tmp/current_results --results-directory /tmp/results_of_comparison

");
}

sub getCmdLineOptions(@)
{
    local (@ARGV) = @_;

    Getopt::Long::config (qw(bundling autoabbrev no_pass_through));

    my $result = GetOptions("t|test-output=s" => \$InputFile,
                            "r|results-directory=s"    => \$TapDirectory, 
                            "c|other-directory=s"    => \$CompareDirectory, 
                            "p|percent-range=i"    => \$PercentRange,
                            "h|help" => sub { usage(); exit 0; },
                            "b|baseline-directory=s" => \$BaselineResults);

    unless ($result) {
        print STDERR "GetOptions Failure\n";
        usage();
        exit 1;
    }
}

sub comparePerformances()
{
    my $comparePrefix = "";
    if (defined $CompareDirectory) {
        print "Comparing results in $BaselineResults and $CompareDirectory\n";
        $comparePrefix = "$CompareDirectory/";
    } else {
        print "Comparing results in $BaselineResults and TAP files in current directory\n";
    }

    my $basedir = DirHandle->new($BaselineResults);
    die "Unable to open baseline directory $BaselineResults\n" if (not defined $basedir);
    my @basefiles;
    while (defined($_ = $basedir->read)) {
        next if ($_ eq ".") or ($_ eq "..");
        push @basefiles, ($_);
    }
    foreach my $filename (@basefiles) {
        my $compareToName = $comparePrefix . $filename;
        open(COMPFILE, $compareToName) or die "Cannot open $compareToName";
        my @lines = <COMPFILE>;          # Read it into an array
        close(COMPFILE) or warn "Failed to close compare-to file=$compareToName";
        my %times32bit;
        my %times64bit;
        foreach my $line (@lines) {
            if ($line =~ m/(\d+) Test (\w+) (\d+) bits timed at (\d+.\d+) sec/) {
                my $test = $2;
                my $bits = $3;
                my $result = $4;
                if ($bits == 32) {
                    $times32bit{$test} = $result;
                } else {
                    $times64bit{$test} = $result;
                }
            }
        }

        my $baselineName = "$BaselineResults/$filename";
        open(BASEFILE, $baselineName) or die "Cannot open $baselineName";
        @lines = <BASEFILE>;          # Read it into an array
        close(BASEFILE) or warn "Failed to close baseline file=$baselineName";

        my $count = 0;
        my $outfile = $FilePrefix . $filename;
        my $fileHandle = FileHandle->new($outfile, O_CREAT | O_TRUNC | O_RDWR);
        die "Cannot open output file $outfile for writing" if not defined $fileHandle;

        foreach my $line (@lines) {
            if ($line =~ m/(\d+) Test (\w+) (\d+) bits timed at (\d+.\d+) sec/) {
                my $test = $2;
                my $bits = $3;
                # allow for both printf's rounding and --percent-range
                my $baseResult = ($4 + 0.001) * $UpperPerfRange;
                my $okNotOk;
                my $compareTo;
                my $skip = 0;      # skip outputting this line
                my $beyondWithin;  # "beyond" or "within" for pretty-print 
                if ($bits == 32) {
                    $compareTo = $times32bit{$test};
                } else {
                    $compareTo = $times64bit{$test};
                }
                if (not defined $compareTo) {  # There in baseline but missing in new, so OK
                    $skip = 1;
                } elsif ($compareTo > $baseResult) {
                    $okNotOk = "not ok";
                    $beyondWithin = "beyond";
                    $count++;
                } else {
                    $okNotOk = "ok";
                    $beyondWithin = "within";
                    $count++;
                }
                if (not $skip) {
                    print $fileHandle "$okNotOk $count Test $test $bits bits timed at $compareTo $beyondWithin allowed limit of $baseResult sec\n";
                }
            }
        }
        %times32bit = (); # clear for next test file
        %times64bit = (); # clear for next test file
        print $fileHandle "1..$count\n";
        $fileHandle->close();
    } # foreach my $filename

    $basedir->close();
}

sub extractPerformances()
{
    print "Extracting test results from test Output File: $InputFile\n";
    open(INPFILE, $InputFile) or die "Failed to open input file=$InputFile";
    my @lines = <INPFILE>;          # Read it into an array
    close(INPFILE) or warn "Failed to close the summary file=$InputFile";

    my $bits = 0;
    my $prevSuite ="";
    my $needTapSummary = 0;  # 1..n TAP postamble
    my %testCounts = ();   # per-file test counts
    my $fileHandle;
    my $openFlags = O_CREAT | O_TRUNC | O_RDWR;

    foreach my $line (@lines) {
        my $platform = "";
        my $suite = "";
        my $curTest  = "";
        my $timeElapsed = 0.0;

        if ($line =~ m/^make(\s+)PLATFORM_CURRENT=(\w+)/) {
            if ($bits != 0) {                       # Did one platform already, so:
                $openFlags = O_RDWR | O_APPEND;   # do not truncate
                if (defined $fileHandle) {
                    $fileHandle->close();
                    undef $fileHandle;
                }
                $needTapSummary = 1;
            }
            $platform = $2;
            if ($platform eq "i386") {
                $bits = 32;
            } elsif ($platform eq "x86_64") {
                $bits = 64;
            }
            next;
        }
        if ($line =~ m/Test \[(\w+)::(\w+)\]: (\d+.\d+)/) {
            $suite = $1;
            $curTest = $2;
            $timeElapsed = $3;
        } else {
            next;   # Skip line if pattern not matched
        }

        next if ($timeElapsed == 0.0);  # Only process non-zero test results
        next if ($suite =~ m/V2/);  # Get rid of MDBM V2 test results
        next if ($curTest eq "finalCleanup") or ($curTest eq "initialSetup");

        if ((defined $fileHandle) and ($prevSuite ne $suite)) {
            print $fileHandle "1..$testCounts{$prevSuite}\n" if ($needTapSummary == 1);
            $fileHandle->close();
        }

        if ($prevSuite ne $suite) {
            my $filename = $FilePrefix . $suite . "_TAP.txt";
            $fileHandle = FileHandle->new($filename, $openFlags);
            die "Cannot open file $filename for writing" if not defined $fileHandle;
        }

        if (not defined $testCounts{$suite}) {
            $testCounts{$suite} = 1;
        } else {
            ($testCounts{$suite})++;
        }
        my $count = $testCounts{$suite};
        print $fileHandle "ok $count Test $curTest $bits bits timed at $timeElapsed sec\n";
        $prevSuite = $suite;
    }

    # Print last TAP summary and close file (use if defined for safety).
    if (defined $fileHandle) {
        print $fileHandle "1..$testCounts{$prevSuite}\n";
        $fileHandle->close();
    }
}

sub main
{
  getCmdLineOptions(@_);

  my $createResults = 0;

  if (not defined $BaselineResults) {
    if (not defined $TapDirectory) {
        print 
        "One of the following options is required: --baseline-direcotry or --results-directory\n";
        exit(1);
    }
    $createResults = 1;
  }

  $FilePrefix = "$TapDirectory/" if (defined $TapDirectory);

  if (defined $TapDirectory) { # create it, if necessry
    mkdir $TapDirectory;
    print "Storing TAP results in Directory: $TapDirectory\n";
  }

  if (defined($PercentRange)) {
    if ($PercentRange <= 0 || $PercentRange > 100) {
        usage();
        die "Cannot continue. Invalid percent specified=$PercentRange. Valid range [1, 100].";
    }
    $UpperPerfRange = (100 + $PercentRange) / 100;
  }

  if ($createResults == 0) {
    comparePerformances();
  } else {
    if (not defined $InputFile or not -e $InputFile) {
        print "Cannot find performance test results file\n";
        exit(2);
    }
    extractPerformances();
  }
}

main(@ARGV);

=pod

=head1 Performance Comparison Tool Use cases

=head4 use case 1 - How to generate TAP files by parsing output of tests:

=over 1

=item *
perf_compare.pl --test-output /tmp/make_test.out --results-directory /tmp/current_results

=back

=head4 Typical use case 2 - How to compare results against a baseline:

=over 1

=item *
perf_compare.pl --baseline-directory /tmp/baseline --other-directory /tmp/current_results --results-directory /tmp/results_of_comparison

=back

=cut
