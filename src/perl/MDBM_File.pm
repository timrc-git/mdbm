#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

package MDBM_File;

use strict;
use warnings;
use Carp;

require Exporter;
use AutoLoader;
require Tie::Hash;

our @ISA = qw(Tie::Hash Exporter DynaLoader);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration   use MDBM_File ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.

our %EXPORT_TAGS = ( 'all' => [ qw(
	MDBM_ALIGN_16_BITS
	MDBM_ALIGN_32_BITS
	MDBM_ALIGN_64_BITS
	MDBM_ALIGN_8_BITS
	MDBM_ANY_LOCKS
	MDBM_API_VERSION
	MDBM_BSOPS_FILE
	MDBM_BSOPS_MDBM
	MDBM_CACHEMODE_GDSF
	MDBM_CACHEMODE_LFU
	MDBM_CACHEMODE_LRU
	MDBM_CACHEMODE_NONE
	MDBM_CACHE_MODIFY
	MDBM_CACHE_ONLY
	MDBM_CACHE_REPLACE
	MDBM_CHECK_ALL
	MDBM_CHECK_CHUNKS
	MDBM_CHECK_DIRECTORY
	MDBM_CHECK_HEADER
	MDBM_CLEAN
	MDBM_CLOCK_STANDARD
	MDBM_CLOCK_TSC
	MDBM_COPY_LOCK_ALL
	MDBM_CREATE_V3
	MDBM_DBSIZE_MB
	MDBM_DEFAULT_HASH
	MDBM_DEMAND_PAGING
	MDBM_FETCH_FLAG_DIRTY
	MDBM_HASH_CRC32
	MDBM_HASH_EJB
	MDBM_HASH_FNV
	MDBM_HASH_HSIEH
	MDBM_HASH_JENKINS
	MDBM_HASH_MD5
	MDBM_HASH_OZ
	MDBM_HASH_PHONG
	MDBM_HASH_SHA_1
	MDBM_HASH_STL
	MDBM_HASH_TOREK
	MDBM_INSERT
	MDBM_INSERT_DUP
	MDBM_ITERATE_ENTRIES
	MDBM_ITERATE_NOLOCK
	MDBM_KEYLEN_MAX
	MDBM_LARGE_OBJECTS
	MDBM_LOCKMODE_UNKNOWN
	MDBM_MAGIC
	MDBM_MODIFY
	MDBM_NO_DIRTY
	MDBM_OPEN_NOLOCK
	MDBM_OPEN_WINDOWED
	MDBM_O_ACCMODE
	MDBM_O_ASYNC
	MDBM_O_CREAT
	MDBM_O_DIRECT
	MDBM_O_FSYNC
	MDBM_O_RDONLY
	MDBM_O_RDWR
	MDBM_O_TRUNC
	MDBM_O_WRONLY
	MDBM_PAGESIZ
	MDBM_PARTITIONED_LOCKS
	MDBM_PROTECT
	MDBM_PROT_NONE
	MDBM_PROT_READ
	MDBM_PROT_WRITE
	MDBM_PTYPE_DATA
	MDBM_PTYPE_DIR
	MDBM_PTYPE_FREE
	MDBM_PTYPE_LOB
	MDBM_REPLACE
	MDBM_RESERVE
	MDBM_RW_LOCKS
	MDBM_SINGLE_ARCH
	MDBM_STATS_BASIC
	MDBM_STATS_TIMED
	MDBM_STAT_CB_ELAPSED
	MDBM_STAT_CB_INC
	MDBM_STAT_CB_SET
	MDBM_STAT_CB_TIME
	MDBM_STAT_OPERATIONS
	MDBM_STAT_TYPE_DELETE
	MDBM_STAT_TYPE_FETCH
	MDBM_STAT_TYPE_STORE
	MDBM_STORE_ENTRY_EXISTS
	MDBM_STORE_SUCCESS
	MDBM_VALLEN_MAX
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	MDBM_ALIGN_16_BITS
	MDBM_ALIGN_32_BITS
	MDBM_ALIGN_64_BITS
	MDBM_ALIGN_8_BITS
	MDBM_ANY_LOCKS
	MDBM_API_VERSION
	MDBM_BSOPS_FILE
	MDBM_BSOPS_MDBM
	MDBM_CACHEMODE_GDSF
	MDBM_CACHEMODE_LFU
	MDBM_CACHEMODE_LRU
	MDBM_CACHEMODE_NONE
	MDBM_CACHE_MODIFY
	MDBM_CACHE_ONLY
	MDBM_CACHE_REPLACE
	MDBM_CHECK_ALL
	MDBM_CHECK_CHUNKS
	MDBM_CHECK_DIRECTORY
	MDBM_CHECK_HEADER
	MDBM_CLEAN
	MDBM_CLOCK_STANDARD
	MDBM_CLOCK_TSC
	MDBM_COPY_LOCK_ALL
	MDBM_CREATE_V3
	MDBM_DBSIZE_MB
	MDBM_DEFAULT_HASH
	MDBM_DEMAND_PAGING
	MDBM_FETCH_FLAG_DIRTY
	MDBM_HASH_CRC32
	MDBM_HASH_EJB
	MDBM_HASH_FNV
	MDBM_HASH_HSIEH
	MDBM_HASH_JENKINS
	MDBM_HASH_MD5
	MDBM_HASH_OZ
	MDBM_HASH_PHONG
	MDBM_HASH_SHA_1
	MDBM_HASH_STL
	MDBM_HASH_TOREK
	MDBM_INSERT
	MDBM_INSERT_DUP
	MDBM_ITERATE_ENTRIES
	MDBM_ITERATE_NOLOCK
	MDBM_KEYLEN_MAX
	MDBM_LARGE_OBJECTS
	MDBM_LOCKMODE_UNKNOWN
	MDBM_MAGIC
	MDBM_MODIFY
	MDBM_NO_DIRTY
	MDBM_OPEN_NOLOCK
	MDBM_OPEN_WINDOWED
	MDBM_O_ACCMODE
	MDBM_O_ASYNC
	MDBM_O_CREAT
	MDBM_O_DIRECT
	MDBM_O_FSYNC
	MDBM_O_RDONLY
	MDBM_O_RDWR
	MDBM_O_TRUNC
	MDBM_O_WRONLY
	MDBM_PAGESIZ
	MDBM_PARTITIONED_LOCKS
	MDBM_PROTECT
	MDBM_PROT_NONE
	MDBM_PROT_READ
	MDBM_PROT_WRITE
	MDBM_PTYPE_DATA
	MDBM_PTYPE_DIR
	MDBM_PTYPE_FREE
	MDBM_PTYPE_LOB
	MDBM_REPLACE
	MDBM_RESERVE
	MDBM_RW_LOCKS
	MDBM_SINGLE_ARCH
	MDBM_STATS_BASIC
	MDBM_STATS_TIMED
	MDBM_STAT_CB_ELAPSED
	MDBM_STAT_CB_INC
	MDBM_STAT_CB_SET
	MDBM_STAT_CB_TIME
	MDBM_STAT_OPERATIONS
	MDBM_STAT_TYPE_DELETE
	MDBM_STAT_TYPE_FETCH
	MDBM_STAT_TYPE_STORE
	MDBM_STORE_ENTRY_EXISTS
	MDBM_STORE_SUCCESS
	MDBM_VALLEN_MAX
);

our $VERSION = 2.0;

sub AUTOLOAD {
    # This AUTOLOAD is used to 'autoload' constants from the constant()
    # XS function.

    my $constname;
    our $AUTOLOAD;
    ($constname = $AUTOLOAD) =~ s/.*:://;
    croak "&MDBM_File::constant not defined" if $constname eq 'constant';
    my ($error, $val) = constant($constname);
    if ($error) { croak $error; }
    {
    no strict 'refs';
    # Fixed between 5.005_53 and 5.005_61
#XXX    if ($] >= 5.00561) {
#XXX        *$AUTOLOAD = sub () { $val };
#XXX    }
#XXX    else {
        *$AUTOLOAD = sub { $val };
#XXX    }
    }
    goto &$AUTOLOAD;
}

require DynaLoader;
__PACKAGE__->bootstrap($VERSION);

# MDBM V3 uses XSLoader.  MDBM V4 uses DynaLoader to load RTLD_GLOBAL|RTLD_NOW due to lock plugin.
# MDBM V4 has a plugin architecture so we don't have to depend on proprietary locks, 
# but platform makefiles require us to provide useless stubs. If libmdbm isn't fully
# loaded/constructed with it's symbols global, before the plugin is loaded, 
# then it will use the broken stubs and fail to work.
# We therefore need DynaLoader to load MDBM_File with options RTLD_GLOBAL|RTLD_NOW.
# By default DynaLoader will load any module with RTLD_LAZY, which would MDBM fail to use the
# lock plugin.
# We also need any module that manually dl_load_file(MDBM_File.so, dl_load_flags()), to get "1"
# when calling dl_load_flags() so that it will load MDBM_File with RTLD_GLOBAL|RTLD_NOW.

# Notify any modules to load MDBM_File with RTLD_GLOBAL|RTLD_NOW
sub dl_load_flags { 0x1 }

# Notify PERL's DynaLoader to load the MDBM_File module with RTLD_GLOBAL|RTLD_NOW
sub dl_loadflags { 0x1 }

# Preloaded methods go here.
    
%MDBM_File::shake_func_hash = ();
%MDBM_File::shake_obj_hash = ();
    
sub limit_size_v3($$;$$)
{   
    my ($self, $size, $shake_func, $shake_obj ) = @_;
    if (defined($shake_func)) {
        my $hashId = getHashId($self);
        clean_shake_entries($self);
        $MDBM_File::shake_func_hash{$hashId} = $shake_func;
        $MDBM_File::shake_obj_hash{$hashId} = $self;
        MDBM_File::limit_size_v3_xs($self, $size, 1);
    } else {
        MDBM_File::limit_size_v3_xs($self, $size, 0);
    }
}

sub pl_mdbm_shake_v3_callback($$$;$) 
{
    my ($shake_data, $key, $val, $mdbm) = @_;

    croak "NULL V3 shake data" if (not defined($mdbm));

    my $cb = $MDBM_File::shake_func_hash{getHashId($mdbm)};
    my $obj = $MDBM_File::shake_obj_hash{getHashId($mdbm)};

    return &$cb($obj, $key, $val, $shake_data);
}

# To get rid of warnings such as below, call this subroutine before untie-ing:
# untie attempted while 1 inner references still exist at ..., line 1234

sub clean_shake_entries($)
{
    my ($self) = @_;
    my $hashId = getHashId($self);

    if (exists($MDBM_File::shake_func_hash{$hashId})) {  # delete from both hashes
        delete($MDBM_File::shake_func_hash{$hashId});
        delete($MDBM_File::shake_obj_hash{$hashId});
    }
}

sub UNTIE($)
{   
    my ($self) = @_;
    clean_shake_entries($self);
}

1;
__END__

=head1 NAME

MDBM_File - Tied access to mdbm files

=head1 SYNOPSIS

  use MDBM_File;
  use Fcntl;

  my $mdbm_obj = tie(%h, 'MDBM_File', 'file.mdbm', MDBM_O_RDWR|MDBM_O_CREAT, 0640 );

  undef($mdbm_obj);
  untie %h;

  tie(%h, 'MDBM_File', 'file.mdbm', O_RDWR|O_CREAT, 0640, $pagesize, $dbsize );
  my $mdbm_obj = tied %h; # You can also get this as the response from tie() above
  $mdbm_obj->lock();
  $h{'key'}++;
  $mdbm_obj->unlock();
  delete $h{'key'};

  untie %h;

=head1 DESCRIPTION

See L<perlfunc/tie> and also L<perltie> (see L<perltie/The C<untie> Gotcha>
for why the explicit undef()'s in the examples above are needed). This module
implements a tied hash map over an MDBM file, providing both the high level
hash perl object, as well as direct access to the underlying MDBM methods.

For more information about MDBM, read the documentation on RTFM in the gendoc folder.

Most MDBM functions from the C-library is available in the Perl module. The
main difference is that the name drops the `mdbm_' prefix. For example

    $mdbm_obj->lock();
    $mdbm_obj->plock("leif", 0);


and so on. The main APIs not supported include the "iterator" functions,
since you can do that easier / cleaner using the Perl mechanisms for
iterating over the hash.

=head2 MDBM V3 Shake function: second argument of limit_size_v3()

You can set the size limit on your MDBM file, and provide a Perl function
to be called to clean an MDBM page (the shake function) when you can no longer
successfully store data in the MDBM. This function takes four arguments:

=over

=item DB (MDBM_File object)

A reference to the MDBM_File object (basically, "self").

=item Key (datum)

The key of the MDBM entry being added that is about to fail to be stored.

=item Value (datum)

The value of the MDBM entry being added that is about to fail to be stored.

=item Object 

Some anonymous object reference you get in your callback from argument 3 of limit_size_v3().

=back

=head3 Example

  sub shake_me($$$$)
  {
    my ($db, $key, $val, $shakeObj) = @_;

    my $items = $shakeObj->getCount();
    my $idx = 0;
    my $toDelete = $shakeObj->getSpaceNeeded();

    my ($curKey, $curVal, $freeUp);
    while (($idx < $items) and ($toDelete > 0)) {
      $curKey = $shakeObj->getPageEntryKey($idx);
      $curVal = $shakeObj->getPageEntryValue($idx);

      # the "if" statement below decides whether to delete a particular entry
      # on the page: delete every 3rd entry that's not "Odin".
      if (($curKey ne "Odin") and $idx % 3 == 0) {
        $freeUp = length($curKey) + length($curVal);
        $shakeObj->setEntryDeleted($idx);
        $toDelete -= $freeUp;
      }
      $idx++;
    }
    return 1;  # Changes were made: items were deleted means we should return 1
  }

  ...

  $mdbm_obj->limit_size_v3(123, \&shake_me, $blah);

=head2 Prune functions

You can also explicitly prune your MDBM file, using a mechanism similar
to the shake function. Your prune function does not take an urgency
argument, instead you can pass some data (a string) along in the callback
chain.

=head3 Example

  sub prune_me
  {
    my ($db, $key, $val, $param) = @_;

    return (int($val) > int($param)) ? 1 : 0;
  }

  $mdbm_obj->prune(\&prune_me, "123");

=head2 Iterators

You can use the MDBM iterators, by first instantiating an iterator object
which is subsequently used by all _r functions. The name of this class is
MDBM_Iter, e.g. "my $iter = new MDBM_Iter;".

=head3 Example

$iter = new MDBM_Iter; # This is reset by default;
while ($val = $mdbm->fetch_r("loki", $iter)) {
  print "Value is $val\n";
}

You can reset an iterator explicitly (between uses), e.g.

$iter->reset();

=head2 EXPORT

None by default.

=head2 Exportable constants
  MDBM_DEFAULT_HASH
  MDBM_HASH_CRC32
  MDBM_HASH_EJB
  MDBM_HASH_FNV
  MDBM_HASH_JENKINS
  MDBM_HASH_MD5
  MDBM_HASH_OZ
  MDBM_HASH_PHONG
  MDBM_HASH_SHA_1
  MDBM_HASH_STL
  MDBM_HASH_TOREK
  MDBM_INSERT
  MDBM_INSERT_DUP
  MDBM_LARGE_OBJECTS
  MDBM_MAXPAGE
  MDBM_MAX_HASH
  MDBM_MAX_SHIFT
  MDBM_MINPAGE
  MDBM_MIN_PSHIFT
  MDBM_MODIFY
  MDBM_PAGESIZ
  MDBM_PARTITIONED_LOCKS
  MDBM_REPLACE
  MDBM_RESERVE
  MDBM_STORE_MASK
  MDBM_HEADER_ONLY
  MDBM_DBSIZE_MB
  MDBM_RW_LOCKS
  MDBM_CREATE_V2
  MDBM_CREATE_V3
  MDBM_OPEN_NOLOCK

=head1 SUPPORT

Contact the mdbm-users mailing list for support.

=head1 SEE ALSO

L<perl>, L<perlfunc/tie> and L<mdbm>.

=head1 BUGS

=cut
