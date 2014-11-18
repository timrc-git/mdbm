.. $Id$
   $URL$

.. _glossary:

Glossary
========

.. When creating glossaries, use the glossary directive
   rather than an ordinary definition list.
   The glossary directive has a couple of key features:
   Sphinx will automatically sort entries if you supply the :sorted: option,
   and any :term: will link back to the appropriate glossary entry here.
   On the flip side, Sphinx will yell at you if you specify a
   :term: that does NOT have a corresponding glossary entry, somewhere.
   You can also supply multiple glossary terms per entry.
   http://sphinx.pocoo.org/markup/para.html#glossary

.. glossary::
    :sorted:

    LOOP
        Large Object or Oversized Page.
        An MDBM must be configured for large objects for there
        to be large object pages.  An oversized page is one that is
        larger than the configured page size.  Oversized pages are
        created by logically appending another page.  This happens
        when a page runs out of space for a store.


    LOS
        Large Object Store.

        * In V2, there was a distinct memory region to store large objects.
          Multiple 4KB block are bound together (as needed) to hold a large object.
          The LOS is limited to 1GB.
        * In V3, there is not a separate memory region.  However, there is only 1
          large object per MDBM page, and multiple consecutive MDBM pages can be bound
          together to has a large object.

    Oversized Page
        Multiple MDBM physical pages that are contiguously bound together as one
        local MDBM page.  Oversized pages are created during a store operation
        when there is insufficient space available on that page.  After all other
        options to reclaim space have been exhausted (compressing the page,
        shaking that page) and oversized page is created.


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: LOS emacsen oversized

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
