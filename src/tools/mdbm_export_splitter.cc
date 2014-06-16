/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/**
 * Given a set of source files(cdb or db_dump), split the contents into sorted
 * (via simple hash) bucket files for use by mdbm_import in an efficient way.
 * The source files may be specified by -i and -p options, as well as listed
 * on the command line.
 *
 * The created buckets will not contain a header if in db_dump format.
 *
 * OPTIONS:
 * -i <input dir> : This is where the source cdb or db_dump files are.
 * -o <out dir> : This is where to build the bucket files.
 *                REQUIRED.
 * -b <cnt> : This is the number of buckets to build.
 *            Default number of buckets is 50.
 * -p <source file prefix> : This is the prefix to identify the source files
 *                           in the input directory.
 * -c : This specifies that the source input files are in cdb format. 
 *      Default format is db_dump.
 * --hash-function <hash code> : This number identifies the hash function to use.
 *                  REQUIRED.
*/

#include <getopt.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <dirent.h>
#include <ctype.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "mdbm.h"

using namespace std;

bool   CdbFlag  = false;
int    HashCode = -1;  // REQUIRED
int    BuckCnt  = 50;
string InputDir;
string SrcFilePrefix;
string OutputDir = ".";

// Given a string, return the appropriate hash code value.
// The string may be the number of the hash code or a name of a hash code.
int
getNumForHashCode(const char *optarg)
{
    string str(optarg);
    int smallestHashCode = 0;
    int biggestHashCode  = 10;
    string nums("0123456789");
    size_t foundNum = str.find_first_of(nums);
    if (foundNum != string::npos) // have a number
    {
        // exceptions are "CRC32", "MD5" and "SHA1" since they contain numbers
        if (foundNum == 0) // OK should be a number and not a name
        {
            int hcode = atoi(optarg);
            // catch invalid strings like "1crc32", "010" 
            if (hcode != biggestHashCode && str.size() > 1)
            {
                hcode = -1;
	    }
            if (hcode < smallestHashCode || hcode > biggestHashCode)
            {
                cerr << "ERROR: Bad hash code number specified"
                     << ", hash-function=" << optarg
                     << endl;
                hcode = -1;
            }
            return hcode;
        }
    }

    // Lets translate the name to a number, index equals hash code value
    // so order is important!
    // 0=CRC32 1=EJB 2=PHONG 3=OZ 4=TOREK 5=FNV 6=STL 7=MD5 8=SHA1 9=Jenkins 10=Hsieh
    string nameList[] = { string("CRC"),      string("EJB"),   string("PHONG"), 
                          string("OZ"),       string("TOREK"), string("FNV"), 
                          string("STL"),      string("MD5"),   string("SHA1"), 
                          string("JENKINS"),  string("HSIEH"), string() };

    char (*pfunc)(char) = reinterpret_cast<char(*)(char)>(static_cast<int(*)(int)>(toupper));
    transform(str.begin(), str.end(), str.begin(), pfunc);
    for (int hcode = smallestHashCode; nameList[hcode].empty() == false; ++hcode)
    {
        if (str.find(nameList[hcode]) != string::npos)
        {
            return hcode;
        }
    }
    cerr << "ERROR: Bad hash code specified"
         << ", hash-function=" << optarg
         << endl;
    return -1;
}

void
usage()
{
    cerr << "Usage: mdbm_export_splitter [options] [source files]..." << endl;
    cerr << "Splits the source files into buckets of key/values by hash of the key." << endl;
    cerr << "All source files will be expected to be in the same format, either CDB or db_dump." << endl;
    cerr << "The bucket files will be in the same format as the source files." << endl;
    cerr << "Options:" << endl;
    cerr << "  -c                 The source input files will be in CDB format." << endl;
    cerr << "  -b <bucket count>  Number of buckets to create." << endl;
    cerr << "  --hash-function <hash code> MDBM specific code representing a hash method." << endl;
    cerr << "      User may provide the number or name as shown below:" << endl;
    cerr << "      0  or  CRC32" << endl
         << "      1  or  EJB" << endl
         << "      2  or  PHONG" << endl
         << "      3  or  OZ" << endl
         << "      4  or  TOREK" << endl
         << "      5  or  FNV32" << endl
         << "      6  or  STL" << endl
         << "      7  or  MD5" << endl
         << "      8  or  SHA1" << endl
         << "      9  or  Jenkins" << endl
         << "     10  or  Hsieh" << endl;
    cerr << "  -h                       Show usage and exit." << endl;
    cerr << "  -i <input directory>     Directory that contains the source files in CDB or db_dump format" << endl;
    cerr << "  -o <output directory>    Directory inwhich to write the bucket files" << endl;
    cerr << "  -p <source file prefix>  Only use the source files in the specified input directory that use this prefix" << endl;
}

int
processOptions(int argc, char ** argv)
{
    // get options
    const char *shortOpts = "cb:h:i:o:p:";
    static option longOpts[] = {
        {"hash-function", 1, 0, 0},
        {0, 0, 0, 0}
    }; 
    bool        err_flag  = false;
    for (int copt; (copt = getopt_long(argc, argv, shortOpts, longOpts, NULL)) != -1;)
    {
        switch (copt)
        {
        case 0: 
            HashCode = getNumForHashCode(optarg);
            if (HashCode == -1)
                err_flag = true;
            break;
        case 'b': 
            BuckCnt  = atoi(optarg);
            break;
        case 'c': 
            CdbFlag = true;
            break;
        case 'h': 
            usage();
            exit(0);
            break;
        case 'i': 
            InputDir = optarg;
            break;
        case 'o': 
            OutputDir = optarg;
            break;
        case 'p': 
            SrcFilePrefix = optarg;
            break;
        default:
            err_flag = true;
        }
    }

    int extraArgs = argc - optind;
    err_flag = InputDir.empty() && extraArgs == 0 ? true : err_flag;
    if (err_flag) 
    {
        return -1;
    }
    return 0;
}

class BucketFile
{
public:
    BucketFile(const string &fqname) : _fd(-1), _fqname(fqname) {}
    int write(string &line);

    void close(); // close file

    void operator() (BucketFile *bfile) { if (bfile) bfile->close(); }

    bool   isOpen() { return _fd != -1; }
    string fileName() { return _fqname; }
private:
    int open(); // open or create file

    int    _fd;
    string _fqname; // fully qualified file name path
};

class BucketFiles
{
public:
    BucketFiles(int bucketCnt, const string &outputDir, const string &outFilePrefix);
    BucketFiles(const BucketFiles &buckfile) :
        _bucketCnt(buckfile._bucketCnt), _outputDir(buckfile._outputDir),
        _fnamePrefix(buckfile._fnamePrefix), _buckFiles(buckfile._buckFiles)
    {  }
    virtual ~BucketFiles();

    int count() { return _bucketCnt; }
    int sendToBucket(int buckIndex, string &keyLine, string &valueLine, int valLen);

protected:
    BucketFiles() {}
    int    _bucketCnt;
    string _outputDir;
    string _fnamePrefix;
    vector<BucketFile*> _buckFiles;
};

class CdbBucketFiles : public BucketFiles
{
public:
    CdbBucketFiles(int bucketCnt, const string &outputDir, const string &outFilePrefix) :
        BucketFiles(bucketCnt, outputDir, outFilePrefix)
    {}
    ~CdbBucketFiles();

    struct CdbFinalizeFile
    {
        void operator() (BucketFile *bfile);
    };
};
void CdbBucketFiles::CdbFinalizeFile::operator() (BucketFile *bfile)
{
    if (bfile == NULL)
    {
        return;
    }
    string endoffile = ""; // mdbm_import requires extra \n at end of cdb file
    bfile->write(endoffile);
    bfile->close();
}

class SplitFile
{
public:
    SplitFile(uint32_t hashCode, BucketFiles *buckFiles) :
        _hashCode(hashCode), _buckFiles(buckFiles)
    { }

    SplitFile(const SplitFile &sfile) : _hashCode(sfile._hashCode), _buckFiles(sfile._buckFiles)
    { }

    virtual ~SplitFile() { delete _buckFiles; _buckFiles = NULL; }

    SplitFile& operator= (SplitFile &src);

    virtual void split(string &srcfile) = 0;
    void hashAndSend(const string &key, string &keyLine, string &valueLine, int lineCnt, const string &fname);

private:
    uint32_t     _hashCode;
    BucketFiles *_buckFiles;
};

class SplitterFunc
{
public:
    SplitterFunc(SplitFile *fileSplitter) : _fileSplitter(fileSplitter)
    {}
    void operator() (string &srcfile);
private:
    SplitFile *_fileSplitter;
};
void SplitterFunc::operator() (string &srcfile)
{
    _fileSplitter->split(srcfile);
}

SplitFile& SplitFile::operator= (SplitFile &src)
{
    _hashCode  = src._hashCode;
    _buckFiles = src._buckFiles;
    return *this;
}
void SplitFile::hashAndSend(const string &key, string &keyLine, string &valueLine, int lineCnt, const string &fname)
{
    datum dkey;
    dkey.dptr  = const_cast<char*>(key.c_str());
    dkey.dsize = key.size();
    uint32_t hashValue = 0;
    int ret = mdbm_get_hash_value(dkey, _hashCode, &hashValue);
    if (ret == -1)
    {
        cerr << "SplitFile: ERROR: Cannot get mdbm_get_hash_value"
             << ", key-size=" << key.size()
             << ", file=" << fname 
             << ", line-number=" << lineCnt
             << endl;
    }
    else
    {
        int buckIndex = hashValue % _buckFiles->count();
        int valLen = valueLine.empty() ? -1 : int(valueLine.size());
        _buckFiles->sendToBucket(buckIndex, keyLine, valueLine, valLen);
    }
}


class CdbSplitFile : public SplitFile
{
public:
    CdbSplitFile(uint32_t hashCode, BucketFiles *buckFiles) :
        SplitFile(hashCode, buckFiles)
    {}
    void split(string &srcfile);
};

class DbDumpSplitFile : public SplitFile
{
public:
    DbDumpSplitFile(uint32_t hashCode, BucketFiles *buckFiles) :
        SplitFile(hashCode, buckFiles)
    {}
    void split(string &srcfile);

private:
    static string DbDumpHeaderFields;
};

string DbDumpSplitFile::DbDumpHeaderFields = "format type mdbm_pagesize mdbm_pagecount HEADER";

// split given file
void DbDumpSplitFile::split(string &srcfile)
{
    // open and parse the src file
    // db_dump file format: even number line is the key, odd number line is value
    ifstream ifs(srcfile.c_str(), ios_base::in);
    bool     noMoreHeader = false;
    int      lineCntMod   = 0;
    string   line;
    for (int cnt = 0; getline(ifs, line); ++cnt)
    {
        if (line.empty())
        {
            continue;
        }
        // parse the key
        string  key     = line;
        string  keyline = line;
        string  valline;
        //int     vallen  = -1;
        
        // eat up db_dump header if not yet done so
        if (noMoreHeader == false)
        {
            // check for header fields and throw away
            bool checkMoreHeader = false;

            stringstream hdrs(DbDumpHeaderFields);
            while (hdrs)
            {
                string field;
                hdrs >> field;
                if (field.empty() == false && line.find(field) != string::npos)
                {
                    // found a header so jump past it
                    checkMoreHeader = true;
                    break;
                }
            }
            if (checkMoreHeader)
            {
                continue; // devoured a header field, continue to check for more
            }
            // HAVE: no more header fields, so process the keys and values
            noMoreHeader = true;
            lineCntMod = cnt % 2;
        }
        if (cnt % 2 == lineCntMod)
        {
            if (getline(ifs, valline))
            {
                ++cnt;
                //vallen = valline.size();
            }
            else
            {
                cerr << "SplitFile: ERROR: Mal-formed db_dump syntax"
                     << ", file=" << srcfile
                     << ", line-number=" << cnt
                     << ", line-size=" << line.size()
                     << endl;
                continue;
            }
        }

        hashAndSend(key, keyline, valline, cnt, srcfile);
    }
}
void CdbSplitFile::split(string &srcfile)
{
    ifstream ifs(srcfile.c_str(), ios_base::in);
    string   line;
    string   valLine;
    for (int cnt = 0; getline(ifs, line); ++cnt)
    {
        if (line.empty())
        {
            continue;
        }
        // parse the key
        string  key     = line;
        string  keyLine = line;
        // ex cdb format: +8,8:HHHHHHH7->HHHHHHH7
        size_t pos  = line.find(':');
        size_t pos2 = line.find("->");
        if (pos != string::npos && pos2 != string::npos)
        {
            // have the dividers between size, key, and value fields
            ++pos;
            key = line.substr(pos, pos2-pos);
        }
        else
        {
            cerr << "CdbSplitFile: ERROR: Mal-formed cdb syntax"
                 << ", file=" << srcfile
                 << ", line-number=" << cnt
                 << ", line-size=" << line.size()
                 << endl;
            continue;
        }

        hashAndSend(key, keyLine, valLine, cnt, srcfile);
    }
}


BucketFiles::BucketFiles(int bucketCnt, const string &outputDir, const string &outFilePrefix) :
    _bucketCnt(bucketCnt), _outputDir(outputDir), _fnamePrefix(outFilePrefix),
    _buckFiles(bucketCnt, static_cast<BucketFile*>(NULL))
{
    // NOTE: our bucketFiles has been set to a particular size
    for (int index = 0; index < bucketCnt; ++index)
    {
        ostringstream ssname;
        ssname << _outputDir << "/" << _fnamePrefix;
        ssname.width(3);  // add space for the index digits
        ssname.fill('0'); // 0 fill the extra space
        ssname << index;  // replace the '0's on the end with the index
        _buckFiles[index] = new BucketFile(ssname.str());
    }
}
CdbBucketFiles::~CdbBucketFiles()
{
    CdbFinalizeFile cdbff;
    for_each(_buckFiles.begin(), _buckFiles.end(), cdbff);
}
BucketFiles::~BucketFiles()
{
    string nofile;
    BucketFile bucketCloser(nofile);
    for_each(_buckFiles.begin(), _buckFiles.end(), bucketCloser);
}
/**
  Given the bucketIndex, write the specified key and value to the appropriate
  bucket file. Key and value will get a '\n' added here.
  CDB format will specify a value of NULL.
  db_dump format will specify both a key and a value.
**/
int
BucketFiles::sendToBucket(int buckIndex, string &keyLine, string &valueLine, int valLen)
{
    // choose bucket file
    if (buckIndex >= _bucketCnt)
    {
        cerr << "BucketFiles: ERROR: sendToBucket: Failed to write line(s)"
             << " due to bad index."
             << ", bucket-index=" << buckIndex
             << ", maximum-number-buckets=" << _buckFiles.size()
             << endl;
        return -1;
    }
    BucketFile *bucket = _buckFiles[buckIndex];

    if (!bucket)
    {
        cerr << "BucketFiles: ERROR: sendToBucket: Failed to write line(s)"
             << " because bucket is missing"
             << ", bucket-index=" << buckIndex
             << endl;
        return -1;
    }

    // append the keyline to the file, then if value append the value
    int ret = bucket->write(keyLine);
    if (ret != -1 && valLen > -1)
    {
        ret = bucket->write(valueLine);
    }
    return ret;
}


int
BucketFile::write(string &line)
{
    if (open() == -1) // open the bucket file if not yet created or opened
    {
        cerr << "BucketFile: ERROR: Failed to write line to bucket due to open error"
             << ", file-name=" << _fqname
             << endl;
        return -1;
    }

    line += "\n";

    int ret      = -1;
    int maxTries = 3;
    for (int cnt = 0; cnt < maxTries && (ret = ::write(_fd, line.c_str(), line.size())) == -1; ++cnt)
    {
        int errnum = errno;
        if (errnum == EAGAIN)
        {
            cerr << "BucketFile: WARNING: Would block writing to bucket"
                 << ", bucket-file=" << _fqname
                 << ", try-count=" << cnt
                 << endl;
        }
        else
        {
            cerr << "BucketFile: ERROR: Failed to write line to bucket"
                 << ", bucket-file=" << _fqname
                 << ", errno=" << errnum
                 << ", fd=" << _fd
                 << endl;
            return ret;
        }
    }
    if (ret == -1)
    {
        cerr << "BucketFile: ERROR: Failed to write line to bucket"
             << ", bucket-file=" << _fqname
             << ", fd=" << _fd
             << endl;
    }
    return ret;
}
int
BucketFile::open()
{
    if (_fd != -1)
    {
        return _fd;
    }

    // create file if doesn't exist, else open and seek to the end
    // flags : O_CREAT | O_NONBLOCK | O_APPEND; mode : 0666
    if ((_fd = ::open(_fqname.c_str(), O_CREAT | O_NONBLOCK | O_RDWR | O_APPEND, 0666)) == -1)
    {
        int errnum = errno;
        cerr << "BucketFile: ERROR: Failed to open bucket file"
             << ", file-name=" << _fqname
             << ", errno=" << errnum
             << endl;
        return -1;
    }
    return _fd;
}

void
BucketFile::close() // close file
{
    if (_fd != -1)
    {
        ::close(_fd);
    }
    _fd = -1;
}

int
makeSrcFileList(vector<string> &srclist, string &inputdir, string &srcprefix)
{
    int ret = 0;

    if (inputdir.empty())
    {
        cout << "INFO: makeSrcFileList: no input source directory, "
             << "expecting source files specified on the command line" << endl;
        return ret;
    }

    // lets fill the srclist with names of src files
    // open the directory and get the list of files
    DIR *dirp;
    if ((dirp = opendir(inputdir.c_str())) == NULL)
    {
        cerr << "ERROR: makeSrcFileList: Cannot open directory"
             << ", directory-name=" << inputdir
             << endl;
        return -1;
    }

    struct dirent *direntp;
    while ((direntp = readdir(dirp)) != NULL)
    {
        string fname = direntp->d_name;
        string fqpath = inputdir + "/";
        if (srcprefix.empty())
        {
            srclist.push_back(fqpath + fname);
        }
        else if (fname.find(srcprefix) != string::npos)
        {
            srclist.push_back(fqpath + fname);
        }
    }

    closedir(dirp);
    return ret;
}

int
main(int argc, char **argv)
{
    if (processOptions(argc, argv) == -1)
    {
        usage();
        cerr << "mdbm_export_splitter: exiting..." << endl;
        return 1;
    }

    vector<string> srclist; // will contain fully qualified input file paths if specified on cmd line
    for (int cnt = optind; cnt < argc; ++cnt)
    {
        string fqname = argv[cnt];
        srclist.push_back(fqname); 
    }

    // add to list of files based on input source directory and source file name prefix
    makeSrcFileList(srclist, InputDir, SrcFilePrefix);
    if (srclist.empty())
    {
        cerr << "mdbm_export_splitter: ERROR: Nothing to split, no source files found"
             << " in the input directory nor on command line"
             << ", input-directory=" << InputDir
             << ", source-file-prefix=" << SrcFilePrefix
             << endl;
        usage();
        return 1;
    }

    SplitFile *fileSplitter;
    if (CdbFlag)
    {
        BucketFiles *buckFiles = new CdbBucketFiles(BuckCnt, OutputDir, SrcFilePrefix);
        fileSplitter = new CdbSplitFile(HashCode, buckFiles);
    }
    else
    {
        BucketFiles *buckFiles = new BucketFiles(BuckCnt, OutputDir, SrcFilePrefix);
        fileSplitter = new DbDumpSplitFile(HashCode, buckFiles);
    }

    SplitterFunc splitFunc(fileSplitter);
    for_each(srclist.begin(), srclist.end(), splitFunc);
    delete fileSplitter;

    return 0;
}
