/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

#include <stdio.h>

#include "mdbm_cxx.h"

#include <functional>
#include <algorithm>

typedef Mdbm<const char*,int> Db;
typedef Mdbm<const char*,char*> Db1;
typedef Mdbm<const char*,const char*> Db2;

template <class T>
struct print : public std::unary_function<T,void>
{
    print () { }
    void operator() (T x) {
        printf("%s\n",x);
    }
};

template <>
struct print<Db::value_type>
{
    print () { }
    void operator() (const Db::value_type& v) {
        printf("%s\n",v.first());
    }
};

template <>
struct print<Db1::value_type>
{
    print () { }
    void operator() (const Db1::value_type& v) const {
        printf("%s: %s\n",v.first(),v.second());
    }
};

template <>
struct print<Db2::value_type>
{
    print () { }
    void operator() (const Db2::value_type& v) const {
        printf("%s: %s\n",v.first(),v.second());
    }
};

int
main (int argc, char** argv)
{
    Db db;

    db.open(argv[1],Db::ReadWrite|Db::Create);

    for (int i = 2; i < argc; i++) {
        if (argv[i][0] == '-') {
            db.erase(argv[i]+1);
        } else if (argv[i][0] == '+') {
            std::pair<Db::iterator,bool> p = db.insert(Db::value_type(argv[i]+1,i));
        } else {
            Db::iterator j = db.find(argv[i]);
            if (j == db.end()) {
                printf("not found: %s\n",argv[i]);
            } else {
                while (j != db.end()) {
                    printf("found: %s=%d\n",j->first(),j->second());
                    j++;
                }
            }
        }
    }

    for_each(db.begin(),db.end(),print<Db::value_type>());
    printf("lock status: %s\n",db.islocked() ? "locked" : "unlocked");
    db.close();
    
    Db1 db1;
    char buf[8];

    db1.open("db1.mdbm",Db1::ReadWrite|Db1::Create);
    strcpy(buf,"1");
    db1.insert(Db1::value_type("a",buf));
    strcpy(buf,"2");
    db1.insert(Db1::value_type("b",buf));
    strcpy(buf,"3");
    db1.insert(Db1::value_type("c",buf));
    for_each(db1.begin(),db1.end(),print<Db1::value_type>());
}
