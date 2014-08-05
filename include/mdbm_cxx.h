/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#ifndef MDBM_CXX_H_ONCE
#define MDBM_CXX_H_ONCE

#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <string>
#include <string.h>

#include "mdbm.h"

class MdbmBase
{
  public:
    MdbmBase();
    ~MdbmBase();

    enum {
        ReadWrite = MDBM_O_RDWR,
        ReadOnly = MDBM_O_RDONLY,
        Create = MDBM_O_CREAT,
        Trunc = MDBM_O_TRUNC,
        SyncAsync = MDBM_O_ASYNC
    };

    bool open(const char* filename, int flags = ReadWrite, int mode = 0666,
              int pagesize = 0, int dbsize = 0);
    void dbopen(MDBM* db);
    bool sync(int flags = 0);
    void lock() { mdbm_lock(db); }
    bool islocked() { return mdbm_islocked(db); }
    void unlock() { mdbm_unlock(db); }
    void close();

    MDBM* mdbm() const { return db; }

  protected:
    MDBM* db;
};


template <class T>
inline void MdbmDatumSet(datum& d, const T& t)
{
    d.dptr = const_cast<char*>(reinterpret_cast<const char*>(&t));
    d.dsize = sizeof(t);
}

template <>
inline void MdbmDatumSet<char*>(datum& d, char* const &t)
{
    d.dptr = const_cast<char*>(t ? t : "");
    d.dsize = t ? strlen(t)+1 : 1;
}

template <>
inline void MdbmDatumSet<const char*>(datum& d, const char* const &t)
{
    d.dptr = const_cast<char*>(t ? t : "");
    d.dsize = t ? strlen(t)+1 : 1;
}

template <class T>
inline T const & MdbmDatumGet(char* const &ptr)
{
    return *(reinterpret_cast<T*>(ptr));
}

template <>
inline const char* const & MdbmDatumGet<const char*>(char* const &ptr)
{
    return const_cast<const char* const&>(ptr);
}

template <>
inline char* const & MdbmDatumGet<char*>(char* const &ptr)
{
    return ptr;
}



template <class _Key, class _Tp>
class Mdbm : public MdbmBase
{
  public:
    typedef unsigned int size_type;

    class base_type : public datum
    {
      public:
        base_type() { }

        void const * ptr() const { return dptr; }
        size_t size() const { return dsize; }

        void clear() { dptr = NULL; dsize = 0; }
    };

    class key_type : public base_type
    {
      public:
        key_type() { base_type::clear(); }
        key_type(const _Key& k) { MdbmDatumSet(*this,k); }

        const _Key & operator()() const { return MdbmDatumGet<_Key>(base_type::dptr); }
    };

    class data_type : public base_type
    {
      public:
        data_type() { base_type::clear(); }
        data_type(const _Tp& t) { MdbmDatumSet(*this,t); }

        _Tp const & operator()() { return MdbmDatumGet<_Tp>(base_type::dptr); }
        const _Tp & operator()() const { return MdbmDatumGet<const _Tp>(base_type::dptr);}
    };

    typedef std::pair<key_type,data_type> value_type;

    static void clear_value(value_type& v) {
        v.first.clear();
        v.second.clear();
    };

    class iterator_base
    {
        friend class Mdbm;
      public:
        iterator_base() {
            db = NULL;
            lockHeld = false;
        }
        ~iterator_base() { unlock(); }

        kvpair getnext() {
            lock();
            return mdbm_next_r(db,&dbiter);
        }

        void lock() {
            if (!lockHeld) {
                mdbm_lock(db);
                lockHeld = true;
            }
        }

        void unlock() {
            if (lockHeld) {
                mdbm_unlock(db);
                lockHeld = false;
            }
        }

      protected:
        iterator_base(MDBM* _db) {
            db = _db;
            lockHeld = false;
        }

        MDBM* db;
        MDBM_ITER dbiter;
        bool lockHeld;
    };

    class iterator : public iterator_base
    {
        friend class Mdbm;

      public:
        iterator() { clear_value(value); }
        iterator(const iterator& i) { const_assign(i); }
        iterator(iterator& i) { assign(i); }
        ~iterator() { }

        value_type* operator->() { return &value; }
        const value_type* operator->() const { return &value; }

        value_type& operator*() { return value; }
        const value_type& operator*() const { return value; }

        void const_assign(const iterator& i) {
            bool l = iterator_base::lockHeld;
            memcpy(this,&i,sizeof(*this));
            if (!l && i.lockHeld) {
                iterator_base::lock();
            } else {
                iterator_base::lockHeld = l;
            }
        }

        void assign(iterator& i) {
            memcpy(this,&i,sizeof(*this));
            if (iterator_base::lockHeld) {
                i.lockHeld = false;
                clear_value(i.value);
            }
        }

        iterator& operator=(const iterator& i) {
            const_assign(i);
            return *this;
        }

        iterator& operator=(iterator& i) {
            assign(i);
            return *this;
        }

        bool operator==(const iterator& i) const {
            return (*this)->first.ptr() == i->first.ptr()
                && (*this)->second.ptr() == i->second.ptr();
        }

        bool operator!=(const iterator& i) const {
            return (*this)->first.ptr() != i->first.ptr()
                || (*this)->second.ptr() != i->second.ptr();
        }

        void operator++() {
            set(iterator_base::getnext());
        }

        iterator& operator++(int postfix) {
            set(iterator_base::getnext());
            return *this;
        }

        void unlock() {
            iterator_base::unlock();
            clear_value(value);
        }

      protected:
        iterator(MDBM* _db) : iterator_base(_db) {
            clear_value(value);
        }

            void set(const kvpair& _kv) {
                *((datum*)&value.first) = _kv.key;
                *((datum*)&value.second) = _kv.val;
            }

      protected:
            value_type value;
    };

    typedef const iterator const_iterator;

  public:
    iterator begin() {
        iterator i(db);
        i.lock();
        i.set(mdbm_first_r(db,&i.dbiter));
        return i;
    }
    const_iterator begin() const {
        iterator i(db);
        i.lock();
        i.set(mdbm_first_r(db,&i.dbiter));
        return i;
    }

    const_iterator end() const {
        return nulliterator;
    }
    iterator end() {
        return nulliterator;
    }

    std::pair<iterator,bool> insert(const value_type& x) {
        std::pair<iterator,bool> p;
        p.first.db = db;
        p.first.lock();
        p.first.value = x;
        p.second =(mdbm_store_r(db,&p.first.value.first,&p.first.value.second,
                                MDBM_INSERT,&p.first.dbiter) == 0);
        return p;
    }

    void erase(iterator pos) {
        mdbm_delete_r(db,&pos.dbiter);
    }
    size_type erase(const key_type& k) {
        size_type erased = 0;
        while (!mdbm_delete(db,k)) {
            erased++;
        }
        return erased;
    }

    void clear() {
        mdbm_purge(db);
    }

    iterator find(const key_type& k) {
        iterator i(db);
        i.value.first = k;
        i.lock();
        mdbm_fetch_r(db,&i.value.first,&i.value.second,&i.dbiter);
        return i;
    }
    const_iterator find(const key_type& k) const {
        iterator i(db);
        i.value.first = k;
        i.lock();
        mdbm_fetch_r(db,&i.value.first,&i.value.second,&i.dbiter);
        return i;
    }

  protected:
    // hide for now -- locking is icky
    _Tp& operator[](const key_type& k) {
        iterator i(db);
        i.value.first = k;
        i.lock();
        if (mdbm_fetch_r(db,&i.value.first,&i.value.second,&i.dbiter) == -1
            && errno == ENOENT)
            {
                _Tp tp;
                data_type tmp(tp);
                i.value.first = k;
                i.value.second = tmp;
                mdbm_store_r(db,&i.value.first,&i.value.second,MDBM_INSERT_DUP,
                             &i.dbiter);
            }
        return i->second();
    }

    iterator nulliterator;
};


#endif

