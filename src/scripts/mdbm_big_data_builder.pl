#!/usr/bin/perl -w

# Copyright 2013 Yahoo! Inc.                                         #
# See LICENSE in the root of the distribution for licensing details. #

=head1 NAME

mdbm_big_data_builder.pl - Program to speedup building of large databases.

=head1 SYNOPSIS

Here are some examples of how to run the script.

Ex: Input files are db_dump format and in /tmp/db_dump and
files specified on the command line: extradatafile1 extradatafile2.
This command will also delete the bucket files after import to the mdbm.

  mdbm_big_data_builder.pl --input-directory /tmp/db_dump --input-prefix bigdata --mdbm jupiter.mdbm --output-directory /tmp/output/data --hash-function fnv -d 4G --delete-intermediate-files extradatafile1 extradatafile2

Ex: Export data to CDB format and use it to build a new mdbm.
This command will retain the bucket files after import to the mdbm.

  mdbm_export -f -c -o /tmp/cdb/inputcdb1 db1.mdbm
  mdbm_export -f -c -o /tmp/cdb/inputcdb2 db2.mdbm
  mdbm_big_data_builder.pl -c --input-directory /tmp/cdb/ --input-prefix inputcdb -mdbm jupiter.mdbm --output-directory /tmp/output/data -s 5

=head1 DESCRIPTION

When the amount of physical memory is significantly smaller than the size of
a dataset, populating an mdbm can take a long time (ex., 90GB dataset) due
to page thrashing. 
This program is used to speedup building of larger-than-physical-memory mdbm
databases from CDB or db_dump format input files.

=head1 SUPPORT

Contact the mdbm-users mailing list for support.

=head1 SEE ALSO

L<mdbm>, L<mdbm_import>, L<mdbm_export>.

=cut

use 5.010;

use strict;
use warnings;
use File::Path qw(mkpath rmtree); # qw(make_path remove_tree);
use FileHandle;
use Getopt::Long;
use File::Basename;
use bignum;


# user may specify input files on command line, so InputDirectory would not be necessary
# options
my $InputDirectory = ".";   # ex: "/tmp/cdb";
my $InputPrefix;      # ex: "bigdata";
my $OutputDirectory;  # ex: "/tmp/split"; ## REQUIRED parameter
my $BucketCnt     = 50;
my $MdbmFile;         # ex: "bigdata.mdbm";
my $DeleteBuckets = 0;

# now for mdbm_import options
my $ImportOptions; # ex: "-f -s $HashCode -S $StoreFlag "
my $CdbFormat       = 0;  # flag - input is in cdb format, default is db_dump
my $DeleteKeys      = 0;  # flag
my $DbSize;            # string - it could have suffix, defaults to megs
my $LargeObjects    = 0;  # flag
my $PageSize;       # ex: "4K";  # string - it could have suffix
my $HashCode  = -1; # numeric; ## REQUIRED parameter
my $StoreFlag = 0;  # numeric
my $MaxPages;          # string - it could have suffix
my $StarterImportOpts = ""; # only used if MaxPages was specified
my $SpillSize;         # string - it could have suffix
my $PerformTiming = 0; # flag - time major processing if turned on

# log for bucket splitting via mdbm_export_splitter: Later it will be parsed
# to summarize the total time of processing.
my $SplitTimerFile   = "bigDataSplitTime";

# log for mdbm_import'ing : it will contain multiple time entries, 1 per timing
# per bucket file. Later it will be parsed to summarize the totals for all imports. 
my $ImportTimerFile  = "bigDataImportTime";

# final log file containing summary of times and processing details - such as
# the number of buckets to import
my $SummaryTimerFile = "bigDataTimeSummary"; # unites all timings and gives totals
my $SumTimerFileHndl; # used for the output file when timing is turned on ($PerformTiming)

sub usage
{
    my $script = basename($0);
    print STDERR ($script . " [options] [input-files...]

Options:

-n, --num-buckets bucket_count, numeric
--input-directory directory, containing input data files in CDB or db_dump format
--input-prefix prefix, prefix of name of input data files
--output-directory directory, used to output the bucket files REQUIRED parameter
--mdbm name, name of mdbm file to be created or have data added if already exists
--delete-intermediate-files flag, delete buckets when done. Default is to keep them. This is a negatable option (ie. --nodelete-intermediate-files)
-c,                  cdb format flag, input data files will be cdb format
-D,                  delete keys flag, used by mdbm_import
-d db size,          used by mdbm_import
-l,                  large object flag, used by mdbm_import
-p page size,        used by  mdbm_import, default is 4096
-s, --hash-function hashcode, REQUIRED parameter, the following numbers or 
                              associated case-insensitive strings may be used:
    0   'CRC'
    1   'EJB'
    2   'PHONG'
    3   'OZ'
    4   'TOREK'
    5   'FNV'
    6   'STL'
    7   'MD5'
    8   'SHA1'
    9   'Jenkins'
    10  'Hsieh'

-S store flag,       used by mdbm_import
    Used by mdbm_import: 0=insert, 1=replace, 2=insert-dup, 3= modify
-y maxpages,         used by mdbm_import
-z spillsize,        used by mdbm_import
--perform-timing     Turn on timing for major processing and produce summary.
-h, --help           Usage of this script
-T, Z, f, 2, 3       These flags are ignored
");
}

sub sizeInBytes($)
{
    my ($sizeParam) = @_;
    my $sizeCalc = 0;
    my ($num, $suffix) = ($sizeParam =~ m/(\d+)([KMGkmg]?)/);

    my %suffixSizes = ( "" => 1, "B" => 1, "K" => 1024, "M" => (1024*1024), "G" => (1024*1024*1024) );
    $suffix = uc($suffix);
    if (defined($suffixSizes{$suffix})) {
        $sizeCalc = $num * $suffixSizes{$suffix};
    } else {
        print "Invalid size suffix [$suffix] specified in $sizeParam";
    }

    return $sizeCalc;
}

# hash code can be a number or name, ex: "FNV"
sub getNumForHashCode($)
{
    my ($hashCode) = @_;

    my %hashNums = ( "0" => 0, "1" => 1, "2" => 2, "3" => 3, "4" => 4,
                     "5" => 5, "6" => 6, "7" => 7, "8" => 8, "9" => 9,
                     "10" => 10 );
    if (defined($hashNums{$hashCode})) {
        return $hashNums{$hashCode};
    }

    # have a string, translate to a number
    my %hashNames = ( "CRC" => 0, "EJB" => 1, "PHONG" => 2, "OZ" => 3, "TOREK" => 4,
                      "FNV" => 5, "STL" => 6, "MD5" => 7, "SHA1" => 8, "JENKINS" => 9,
                      "HSIEH" => 10 );

    $hashCode   = uc($hashCode);
    my $hashNum = -1;
    if (defined($hashNames{$hashCode})) {
        $hashNum = $hashNames{$hashCode};
    }
    return $hashNum;
}

sub parseCommandLine
{
    my $ignoredFlagOpts = 0;
    my $ignoredStrOpts  = "";
    my $ignoredIntOpts  = 0;

    Getopt::Long::Configure("no_ignore_case");
    my $result = GetOptions("n|num-buckets=i" => \$BucketCnt,    # numeric
                        "input-directory=s" => \$InputDirectory,      # string
                        "input-prefix=s"    => \$InputPrefix,      # string
                        "output-directory=s" => \$OutputDirectory,      # string
                        "mdbm=s" => \$MdbmFile, # string
                        "delete-intermediate-files!"    => \$DeleteBuckets, # flag
                        "c" => \$CdbFormat,  # flag - mdbm_import
                        "D" => \$DeleteKeys,    # flag - mdbm_import
                        "d=s" => \$DbSize,   # string - mdbm_import
                        "l" => \$LargeObjects,     # flag - mdbm_import
                        "p=s" => \$PageSize, # string - mdbm_import
                        "s|hash-function=s" => \$HashCode, # string or numeric - mdbm_import
                        "S=i" => \$StoreFlag, # numeric - mdbm_import
                        "perform-timing" => \$PerformTiming,  # flag
                        "y=s" => \$MaxPages,  # string - mdbm_import "z=s" => \$SpillSize, # string - mdbm_import
                        "h|help"    => sub { usage(); exit 0; },
                        "i=s"       => \$ignoredStrOpts,  # string
                        "L=i"       => \$ignoredIntOpts,  # numeric
                        "T|Z|f|2|3" => \$ignoredFlagOpts); # flag

    unless ($result) {
        print STDERR "GetOptions Failure\n";
        usage();
        exit 1;
    }

    if (defined($DbSize)) {
        # if it doesn't contain units suffix, then normalize with an 'M'
        if ($DbSize =~ /(\d+)$/) {
            $DbSize .= "M";
        }
        print "Database size specified=$DbSize\n";
    }

    if (scalar(@ARGV) > 0) {
        print "Input file list= @ARGV \n";
    }

    # verify required parameters have been set
    # OutputDirectory is required parameter
    unless ($OutputDirectory) {
        usage();
        die "Cannot continue. output-directory is a required parameter!";
    }
    # HashCode is a required parameter
    # can be a number or name, ex: "FNV"
    if (getNumForHashCode($HashCode) == -1) {
        usage();
        die "Cannot continue. hash-function is a required parameter!";
    }
    $HashCode = getNumForHashCode($HashCode);
}

sub splitExportFiles
{
    # setup the options splitter needs
    # $InputDirectory          # ex: "/tmp/cdb";
    # $InputPrefix         // ex: "bigdata";
    # $OutputDirectory   = ""; // ex: "/tmp/split";  ##  REQUIRED param
    # $BucketCnt  = 50;
    # $CdbFormat = 0;  # flag - input is in cdb format, default is db_dump
    # $HashCode  = -1;  ##  REQUIRED param

    # determine if output directory exists or not, if not lets create it
    unless (-e $OutputDirectory) {
        mkpath($OutputDirectory);
    }

    my $splitterOpts = "-o $OutputDirectory -b $BucketCnt --hash-function $HashCode ";

    if (defined($InputDirectory)) {
        $splitterOpts .= "-i $InputDirectory ";
    }
    if (defined($InputPrefix)) {
        $splitterOpts .= "-p $InputPrefix ";
    }
    if ($CdbFormat != 0) {
        $splitterOpts .= "-c ";
    }

    my $splitCmd = "mdbm_export_splitter $splitterOpts @ARGV";
    print "Run: $splitCmd\n";

    if ($PerformTiming) {
        `date > $SplitTimerFile`;
        my $cmd = "bash -c 'time $splitCmd' >> $SplitTimerFile 2>&1";
        $splitCmd = $cmd;
    }

    system($splitCmd) == 0 or
        die "mdbm_export_splitter failed to split the source files to buckets";

    if ($PerformTiming) {
        `date >> $SplitTimerFile`;
    }
}

# setup options for mdbm_import
sub setupImportOptions
{
    # HashCode is a required parameter
    $ImportOptions = "-f -3 -s $HashCode -S $StoreFlag ";

    if ($DeleteKeys > 0) {
        $ImportOptions .= "-D ";
    }
    if ($LargeObjects > 0) {
        $ImportOptions .= "-l ";
    }

    if ($CdbFormat == 0) {
        $ImportOptions .= "-T "; # splitter outputs db_dump files without headers
    } else {
        $ImportOptions .= "-c "; # splitter output cdb files
    }

    # $PageSize  = "4K";  # string - it could have suffix
    my $pageSizeCalc = 4096;
    if (defined($PageSize)) {
        $ImportOptions .= "-p $PageSize ";
        $pageSizeCalc = sizeInBytes($PageSize);
    } else {
        $ImportOptions .= "-p $pageSizeCalc ";
    }

    # db size = 2^^34 is too big, so should be 2^^33
    my $maxNumPagesAllowed = 16777216;
    my $maxPagesCalc       = $maxNumPagesAllowed;
    # max pages will only be set upon creation of the DB, so set the
    # variable $StarterImportOpts to be used Only for creation of the DB
    if (defined($MaxPages)) {
        # check the MaxPages against $maxNumPagesAllowed
        $maxPagesCalc = sizeInBytes($MaxPages);
        if ($maxPagesCalc > $maxNumPagesAllowed) {
            warn "Maximum specified number pages=$MaxPages bigger than allowed maximum number of pages=$maxNumPagesAllowed";  
            $maxPagesCalc = $maxNumPagesAllowed;
            $MaxPages = $maxPagesCalc;
            warn "Maximum number of pages reset to $MaxPages";
        } else {
            $maxPagesCalc = $MaxPages;
        }
        $StarterImportOpts .= "-y $MaxPages ";
    }

    # max db possible is 255TB according to MDBM V3 twiki
    # note that 255TB is less than 16777216 max-pages * 16M max-pagesize
    my $maxDbSizePossible = 255 * 1024 * 1024 * 1024 * 1024;
    my $calcDbPreSize = $pageSizeCalc * $maxPagesCalc;
    if ($calcDbPreSize > $maxDbSizePossible) {
        $calcDbPreSize = $maxDbSizePossible;
    }
    if (defined($DbSize)) {
        my $dbsizebytes = sizeInBytes($DbSize);
        if ($dbsizebytes > $calcDbPreSize) {
            warn "Maximum number pages=$maxPagesCalc of size=$pageSizeCalc conflicts with specified db size=$dbsizebytes bytes, continuing...";
        }
        $dbsizebytes = int($dbsizebytes / (1024 * 1024)); # want it Meg's
        $DbSize = $dbsizebytes;
    } else {
        $DbSize = int($calcDbPreSize / (1024 * 1024)); # want it Meg's
    }

    if ($DbSize == 0) {
        $DbSize = 1;
    }
    $StarterImportOpts .= "-d $DbSize "; # mdbm_import uses it upon creation of db

    if (defined($SpillSize)) {
        $ImportOptions .= "-z $SpillSize ";
    }
}

sub importFilesAndBuildMdbm
{
    # For target MDBM, if already exists then use it, else it will be created
    # by mdbm_import.
    unless ($MdbmFile) {
        $MdbmFile = "$OutputDirectory/bigdata.mdbm";
    }

    unless (-e $MdbmFile) {
        # verify if it will be created in the $OutputDirectory
        unless ($MdbmFile =~ /\//) {
            my $dbFile = $OutputDirectory . "/" . $MdbmFile;
            $MdbmFile = $dbFile;
        }
        print "Specified mdbm db file=$MdbmFile\n";
    }

    my @bucketList;
    if ($InputPrefix) {
        @bucketList = glob "$OutputDirectory/$InputPrefix" . "*";
    } else {
        @bucketList = glob "$OutputDirectory/*";
    }

    my $splitBucketCnt = @bucketList;
    print "Import buckets(count=$splitBucketCnt) to the DB:\n";

    my $impCmdPrefix = "mdbm_import $ImportOptions ";
    my $impCmdSuffix = "";
    if ($PerformTiming) {
        my $cmd = "bash -c 'time $impCmdPrefix ";
        $impCmdPrefix = $cmd;
        $impCmdSuffix = "' >> $ImportTimerFile 2>&1";
        print $SumTimerFileHndl "Number of buckets to import=$splitBucketCnt\n";
        print $SumTimerFileHndl "Import command line options: $impCmdPrefix $StarterImportOpts $MdbmFile'\n";

        `date > $ImportTimerFile`;
    }

    foreach my $bucket (@bucketList) {
        my $importCmd = "$impCmdPrefix $StarterImportOpts -i $bucket $MdbmFile $impCmdSuffix";
        print "$importCmd\n";
        my $ret = system($importCmd);
        if ($ret == 0) {
            $StarterImportOpts = ""; # only used upon first invocation of mdbm_import
            if ($DeleteBuckets != 0) {
                print "Delete the bucket($bucket)\n";
                unlink $bucket or warn "Failed to unlink the bucket file $bucket";
            }
        } else {
            warn "mdbm_import failed to import bucket( $bucket ) into mdbm( $MdbmFile )";
        }
    }

    if ($PerformTiming) {
        `date >> $ImportTimerFile`;
    }
}

sub sumTimesFromFile($)
{
    my ($timingFile) = @_;
   
    my $realTimeSum = 0.0;
    my $userTimeSum = 0.0;
    my $sysTimeSum  = 0.0;

    open(OUTPUT, $timingFile);
    my @lines = <OUTPUT>;
    close OUTPUT;

    foreach my $line (@lines) {
        if ($line =~ m/^real\s+(\d+)m(\d+.\d+)s.*/) {
            my $realTime = ($1 * 60) + $2;
            $realTimeSum += $realTime;
        }
        if ($line =~ m/^user\s+(\d+)m(\d+.\d+)s.*/) {
            my $userTime = ($1 * 60) + $2;
            $userTimeSum += $userTime;
        }
        if ($line =~ m/^sys\s+(\d+)m(\d+.\d+)s.*/) {
            my $sysTime = ($1 * 60) + $2;
            $sysTimeSum += $sysTime;
        }
    }

    my $totals = "Totals(seconds): real-time=$realTimeSum user-time=$userTimeSum sys-time=$sysTimeSum\n";
    return $totals;
}

sub parseTime($$)
{
    my ($timingStr, $matcher) = @_;

    my $time = 0;
    if ($timingStr =~ m/.+$matcher=(\d+.\d+) .+/) {
        $time = $1;
    } elsif ($timingStr =~ m/.+$matcher=(\d+.\d+)$/) {
        $time = $1;
    }
    return $time;
}
sub produceTimingSummary
{
    unless ($PerformTiming) {
        return;
    }

    my $dtime = `date`;
    print $SumTimerFileHndl "Completed building at: $dtime\n";

    # ok parse split timing and collect its times
    my $splitTime = sumTimesFromFile($SplitTimerFile);
    print $SumTimerFileHndl "Split bucket time: $splitTime\n";

    # ok parse import timing and collect its times
    my $importTime = sumTimesFromFile($ImportTimerFile);
    print $SumTimerFileHndl "Import buckets time: $importTime\n";

    # calculate total time = split time + import time

    my $realSplitTime = parseTime($splitTime, "real-time");
    my $realImportTime = parseTime($importTime, "real-time");
    my $totalRealTime = $realSplitTime + $realImportTime;
    my $userSplitTime = parseTime($splitTime, "user-time");
    my $userImportTime = parseTime($importTime, "user-time");
    my $totalUserTime = $userSplitTime + $userImportTime;
    my $sysSplitTime = parseTime($splitTime, "sys-time");
    my $sysImportTime = parseTime($importTime, "sys-time");
    my $totalSysTime = $sysSplitTime + $sysImportTime;

    # print summary
    my $totals = "Total(seconds): real-time=$totalRealTime user-time=$totalUserTime sys-time=$totalSysTime\n";
    print $SumTimerFileHndl "Full processing: $totals\n";

    `cat $SummaryTimerFile`;
}

sub main
{
    parseCommandLine();
    if ($PerformTiming) {
        unlink $SplitTimerFile, $ImportTimerFile, $SummaryTimerFile;
        $SumTimerFileHndl = FileHandle->new($SummaryTimerFile, O_TRUNC | O_CREAT | O_RDWR);
        unless (defined $SumTimerFileHndl) {
            die "Failed to open file=$SummaryTimerFile used to collect summary of timings";
        }
        my $dtime = `date`;
        print $SumTimerFileHndl "Begin building at: $dtime\n";
    }

    splitExportFiles();
    setupImportOptions();
    importFilesAndBuildMdbm();

    produceTimingSummary();
}

main();
