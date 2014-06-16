#!/bin/sh
# $Id$
#
# Capture the environment for MDBM bugs.

# Global variables set by getCommand.
mdbm=
mdbmNone=0
mdbmOutput=/tmp/`basename $0`.out
mdbmStatOptions=
mdbmSourceTop=

usage()
{
    cat <<-EOF
Usage: [RUN_MODE=DEV] $0 [options] <mdbm-file>

Options:
    -n
       No mdbm-file is provided.   Use this option to capture the
       enviornment without specific mdbm information.

    -o <output-file>
       Save information to output file.

    -s <mdbm-stat-options>
       Use mdbm-stat-option on mdbm_stat command.
       mdbm-stat-options are used with mdbm_stat for traversing all data.  By
       default, the MDBM is locked.  Provide option value '-L' to not lock the
       MDBM.  Traversing an MDBM while writes are happening might lead to
       inaccurate information, but those inaccuracies are probably not
       significant in most cases.

RUN_MODE=DEV is reserved for development to run directly from sources.
EOF
}

getCommand()
{
    while getopts "hns:o:" option; do
        case $option in
            h)  usage;
                exit 0
                ;;
            n)  mdbmNone=1
                ;;
            s)  mdbmStatOptions=$OPTARG
                ;;
            o)  mdbmOutput=$OPTARG
                ;;
            *)  usage;
                exit 1;
                ;;
        esac
    done

    # Remove the processed options from the input arguments.
    shift $(($OPTIND-1))

    if  [ $mdbmNone -ne 0 ]; then
        if [ $# -eq 0 ]; then
            return 0
        else
            echo "mdbm-file may not be specified with the \"-n\" option" >&2
            exit 1
        fi
    fi

    if [ $# -le 0 ]; then
        echo "mdbm-file must be provided" >&2
        exit 1
    fi

    if [ $# -gt 1 ]; then
        echo "Only one mdbm-file may be provided" >&2
        exit 1
    fi

    mdbm=$1
}

setup()
{
    if [ "$RUN_MODE" = 'DEV' ]; then
        # Use `make' to find the mdbm_stat binary and libmdbm.so
        # gmake must be used for FreeBSD compatibility
        SRCTOP=`gmake --silent value_SRCTOP 2>>/dev/null`
        #echo "SRCTOP=$SRCTOP"
        BINDIR=$SRCTOP/`gmake -silent -C $SRCTOP value_OBJDIR 2>>/dev/null`
        #echo "BINDIR=$BINDIR"
        LIBDIR=$SRCTOP/lib/`gmake --silent -C $SRCTOP/lib value_OBJDIR 2>>/dev/null`
        #echo "LIBDIR=$LIBDIR"
        export LD_LIBRARY_PATH=$LIBDIR:$LD_LIBRARY_PATH
        mdbmStat=$BINDIR/new_mdbm_stat
    else
        mdbmStat=mdbm_stat
    fi

    if [ -n "$mdbm" ]; then
        if [ ! -f "$mdbm" ]; then
            echo "ERROR: \"$mdbm\" is not a regular file" >&2
            exit 1;
        fi

        if [ ! -x "$mdbmStat" ]; then
            echo "ERROR: mdbm_stat file \"$mdbmStat\" does not exist or is not executable" >&2
            exit 1;
        fi
    fi
}


captureEnvironment()
{
    echo "Storing mdbm environment information in $mdbmOutput ..."

    local absPath
    if [ -n "$mdbm" ]; then
        local absDir=`dirname $mdbm`
        pushd $absDir >> /dev/null;
        absDir=`pwd`
        absPath=$absDir/`basename $mdbm`
        popd >> /dev/null;
    fi

    printf "### Base Environment ###\n\n" > $mdbmOutput;

    printf "Date: %s\nHost: %s\n" "`date`" "`hostname`" >> $mdbmOutput
    if [ -n "$mdbm" ]; then
        printf "File: $absPath\n" >> $mdbmOutput
    fi
    printf "User: $USER\n" >> $mdbmOutput

    printf "\n### Platform ###\n\n" >> $mdbmOutput;
    printf "uname -a: " >> $mdbmOutput
    uname -a >> $mdbmOutput 2>&1
    redhatRelease=/etc/redhat-release
    if [ -f "$redhatRelease" ]; then
        printf "\nRedHat Release: " >> $mdbmOutput
        cat $redhatRelease >> $mdbmOutput
    fi

    if [ -n "$mdbm" ]; then
        printf "\n### MDBM Header ###\n" >> $mdbmOutput;
        command="$mdbmStat -H -L $mdbm"
        printf "\nMDBM stat header, (%s):\n" "$command" >> $mdbmOutput
        $command >> $mdbmOutput 2>&1

        printf "\n### MDBM Stat ###\n" >> $mdbmOutput;
        command="$mdbmStat $mdbmStatOptions $mdbm"
        printf "\nMDBM stat data (%s):\n" "$command" >> $mdbmOutput
        ( time $command ) >> $mdbmOutput 2>&1
    fi

    echo "Done."
}

userInfoMessage()
{
    cat <<-EOF

Please attach $mdbmOutput to your bug report and provide the
required information in your ticket.  The required
information is described in tohe getting-help section of the docs.

EOF
}

main()
{
    getCommand $*
    setup
    captureEnvironment
    userInfoMessage
}

main "$@"
exit $?
