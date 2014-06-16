.. $Id$
   $URL$

.. _mdbm_cxx:

C++ API
=======

SYNOPSIS
--------

::

  #include <mdbm_cxx.h>

  template <class _Key, class _Tp> class Mdbm {
      class key_type {
	public:
	  const _Key& operator() () const;
      };

      class data_type {
	public:
	  const _Tp& operator() () const;
      };

      typedef pair<key_type,data_type> value_type;

      class iterator {
      public:
	  iterator ();
	  iterator (const iterator& i);
	  iterator (iterator& i);
	  ~iterator ();

	  value_type* operator-> ();
	  const value_type* operator-> ();

	  value_type& operator* ();
	  const value_type& operator* ();

	  iterator& operator= (const iterator& i);
	  iterator& operator= (iterator& i);

	  bool operator== (const iterator& i) const;

	  void operator++ ();
	  iterator& operator++ (int postfix);

	  void unlock ();
      };

      typedef const iterator const_iterator;

   public:
      iterator begin ();
      const_iterator begin () const;

      const_iterator end () const;
      iterator end ();

      pair<iterator,bool> insert (const value_type& x);

      void erase (iterator pos);
      size_type erase (const key_type& k);

      void clear ();

      iterator find (const key_type& k);
      const_iterator find (const key_type& k) const;
  };

DESCRIPTION
-----------

The C++ interface to mdbm models the interface of the hash_map<>
template, a non-standard part of the STL.  Some of the hash_map
member functions are not currently implemented.  Also, it is
necessary to use the () operator to retrieve the value of the
key_type (e.g., x = i->first() instead of x = i->first) and to
set or retrieve the value of the data_type (e.g., i->second()++).

LOCKING
-------

One advantage of the C++ interface is that it manages locking of
the database internally.  When it returns an iterator (other than
the end()), the database is first locked and that lock is held
for the lifetime of the iterator or until the iterator's unlock()
member is called.

EXAMPLE
-------

::

  typedef Mdbm<int,int> Db;
  Db db;
  string fn;

  fn = yax_getroot();
  fn += "/var/db/foo/db1.mdbm";

  if (!db.open(fn.c_str(),Db::ReadWrite|Db::Create)) {
      cerr << "Unable to create database: " << strerror(errno) << endl;
      exit(2);
  }

  int ndups = 0;
  for (int i = 0; i < 10000; i++) {
      pair<Db::iterator,bool> i = db.insert(Db::value_type(i,i*i));
      if (!i.second) {
	  ndups++;
      }
  }

  for_each(db.begin(),db.end(),print<Db::value_type>());

  db.close();

CONTACT
-------

mdbm-users <mdbm-users@yahoo-inc.com>


.. End of documentation

   emacsen buffer-local ispell variables -- Do not delete.

   === content ===
   LocalWords: Tp bool cerr const cxx emacsen endl errno fn getroot mdbm
   LocalWords: ndups pos strerror yax

   Local Variables:
   mode: text
   fill-column: 80
   indent-tabs-mode: nil
   tab-width: 4
   End:
