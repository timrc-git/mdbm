#  Copyright 2013 Yahoo! Inc.                                         #
#  See LICENSE in the root of the distribution for licensing details. #

# In MDBM v4 (RHEL6 only), only a 64-bit architecture is supported.
#
# This script is expected to be called in the context of a 
# Makefile where the following variables are defined:
#   OBJDIR

use strict;
use Carp;
use ExtUtils::MakeMaker;

#printf("### $0: OBJDIR=%s\n", $ENV{OBJDIR});

#my $ObjDir = $ENV{OBJDIR};
#chomp($ObjDir);
#$ObjDir
#  or die 'OBJDIR environment variable is not defined';

#my $DEPPKGS = 'mdbm_dev';

sub MY::post_initialize {
}

sub MY::const_config {
  my $self = shift;
  my $string = << 'EOT';
TOPDIR=$(shell cd ../.. && pwd)
include $(TOPDIR)/Makefile.base

default-make-target: makemakerdflt
EOT
#  $string .= "DEPPKGS = $DEPPKGS\n";
  $string .= $self->MM::const_config;
#  // 32 bit...
#  $string =~ s/^CC\s+=.*$/CC = $(CXX_COMPILER) -m32/g;
#  $string =~ s/^LD\s+=.*$/LD = $(CXX_COMPILER) -m32 -shared/g;
  $string =~ s/^CC\s+=.*$/CC = $(CXX_COMPILER)/g;
  $string =~ s/^LD\s+=.*$/LD = $(CXX_COMPILER) -shared/g;
  return $string;
}

my $ccflags = '$(COMP_CFLAGS_RELAXED)';
my $ldflags = ' -Wl,--no-as-needed -shared -L/usr/lib64';
# for a small population, they want rpath, but I'm not sure how to get that in here.
# they also want -L/usr/local/lib64
# -Wl,-rpath,$(LIBDIR)';
my $libs = "-L\$(LIBDIR) -L../lib/\$(OBJDIR) -lmdbm -lstdc++ ";

#my $makefileSuffix = ($ObjDir eq '.' ? '' : '.' . $ObjDir);
my $makefileSuffix = '';

# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    # According to the POD, FIRST_MAKEFILE should be used to change the
    # name of the output file (defaults to `Makefile').  This variable
    # doesn't work correctly.  Instead of generating the specified file,
    # it always generates a `Makefile'.
    #
    # However, MAKEFILE does work, but it is supposed to be used with
    # MAP_TARGET when producing a perl binary.  Because a perl binary
    # is not being produced, there is no conflict and MAKEFILE can be
    # used.
    #
    # The FIRST_MAKEFILE setting is retained because the generated
    # Makefile.build uses it.

    # FIRST_MAKEFILE and MAKEFILE are both needed (see above)
    #FIRST_MAKEFILE    => 'Makefile.build' . $makefileSuffix,
    #MAKEFILE          => 'Makefile.build' . $makefileSuffix,

    # Even though only 1 platform is actually used, it's still necessary
    # to to build in a platform-specific manner.  Otherwise,
    # platform-specific derived files could overwrite one another.
    INST_ARCHLIB      => '$(OBJDIR)/blib/arch',
    INST_LIB          => '$(OBJDIR)/blib/lib',


    NAME              => 'MDBM_File',
    VERSION_FROM      => 'MDBM_File.pm', # finds $VERSION
    PREREQ_PM         => {},     # e.g., Module::Name => 1.1
    ($] >= 5.005 ?                 ## Add these new keywords supported since 5.005
     (ABSTRACT_FROM     => 'MDBM_File.pm', # retrieve abstract from module
      AUTHOR            => 'Yahoo! MDBM developers') : ()),
    CCFLAGS           => $ccflags,
    #LIBS              => ":nosearch $libs",
    LDDLFLAGS         => " $ldflags $libs ",
    DEFINE            => '',     # e.g., '-DHAVE_SOMETHING'
    INC               => '-I$(INCDIR) -I../../include',
    'XSOPT'             => '-C++',
    # Unfortunately, there's no interface to place the following objects
    # in platform-specific directories.  We'll have to remove them via
    # an enclosing Makefile for each build.  Otherwise, they will be
    # overwritten and not rebuilt across platform builds.
    OBJECT            => 'MDBM_File.o shakedata.o',
);


package MY; # so that "SUPER" works right

sub test {
  my $inherited = shift->SUPER::test(@_);
  # put local mdbm library path into LD_LIBRARY_PATH for the tests
  # TODO FIXME we should be getting $(OBJDIR) from Makefile.base
  $inherited =~ s/(test_dynamic :: pure_all)\n\t/$1\n\tLD_LIBRARY_PATH=\${LD_LIBRARY_PATH}:..\/lib\/object /;
  $inherited;
}
