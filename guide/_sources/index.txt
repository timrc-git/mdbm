.. $Id$
   $URL$

.. _master-index:


MDBM Developer Guide
====================

.. Use the :hidden: option in any reST book that you plan to generate using the doc tool.
   the doc tool's template supplies a master TOC in the sidebar of every page in the book.
   If you omit the :hidden: option,
   the index page will display the TOC twice:
   once in the sidebar and once in the main content area,
   which makes the index page look silly.
   If you're using reST in some other non-Yahoo! project,
   you probably *don't* want to use :hidden:.
   http://sphinx.pocoo.org/markup/toctree.html

Note: **MDBM 2.x support was terminated on 1-Oct-2012.**
This includes the V2 file format.
If you have V2 files, they must be migrated to V3 file format.

MDBM is a fast dbm (key-value store) clone originally based on Ozan Yigit's sdbm.

This MDBM Developer Guide is a reference for building applications with MDBM
|version|.  This software version is not binary or API backward compatible with
MDBM V3 software, but the APIs are very similar.  This version uses the V3 file
format,.

.. _contents:

Contents
--------

.. The Developer Guide and the C API are published separately via the doc tool.
   Separate publications is necessary because the doc tool can run only 1 plugins (reST
   and doxygen) at a time.


.. toctree::
   :maxdepth: 1

   getting-help
   release_notes
   concepts
   C API <c_api>
   C++ API <mdbm_cxx>
   examples
   utilities
   tools
   faq
   restrictions
   performance
   MDBM Internals <internals>
   glossary

The C API  documentation describes the core interface that is used by other wrappers (C++, Perl).

    - The Perl documentation is in installed module MDBM_file.pm
    - The Java and PHP wrappers are not part of this document because
      they are maintained by other groups.

.. Substitution definitions

   Substitution definitions only apply to the page they are defined on,
   but you can work around this by placing your definitions in an rst_prolog:
   http://sphinx.pocoo.org/config.html?highlight=rst_prolog#confval-rst_prolog

.. |y-im| image:: images/messenger.png

.. |reST| replace:: reStructuredText

.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: Ozan PHP TOC Yigit's api confval cxx dbm devel
   LocalWords: emacsen faq gendoc html im maxdepth mdbm prolog reST rst sdbm svn
   LocalWords: toctree

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
