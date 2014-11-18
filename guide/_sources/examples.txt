.. $Id$
   $URL$

.. _examples:

Examples
========

.. TODO: In the following examples, the `include' directives use the ':literal:'
   option instead of the `:code: c' option because the doc tool servers are awaiting
   a future unscheduled upgrade to ypython26_sphinx-1.1.3, which will have the
   `:code: c' support.

Creating and populating a database
----------------------------------

.. include:: mdbm_ex1.c
   :literal:


Fetching and updating records in-place
--------------------------------------

.. include:: mdbm_ex2.c
   :literal:


Adding/replacing records
------------------------

.. include:: mdbm_ex3.c
   :literal:


Iterating over all records
--------------------------

.. include:: mdbm_ex4.c
   :literal:


Iterating over all records with custom iterator
-----------------------------------------------

.. include:: mdbm_ex5.c
   :literal:


Iterating over all keys with custom iterator
--------------------------------------------

.. include:: mdbm_ex6.c
   :literal:


Iterating over all values for a given key
-----------------------------------------

Generation
~~~~~~~~~~

.. include:: mdbm_ex7_gen.c
   :literal:


Iteration
~~~~~~~~~

.. include:: mdbm_ex7_fetch.c
   :literal:


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: emacsen mdbm

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
