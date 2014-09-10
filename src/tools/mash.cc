/* Copyright 2013 Yahoo! Inc.                                         */
/* See LICENSE in the root of the distribution for licensing details. */

/**
 * mash.cpp implements an MDBM Access Shell (MASH).
 *
 * It supports the mdbm-commands that are quite similar to existing mdbm_* command line utils
 * (e.g. mdbm_fetch, mdbm_create...) as internal commands that start with the lowercase letter m,
 * followed by the command name.  The syntax of these internal commands is available after you
 * type "mash" by using the standard option for help under Unix: -h or --help.
 * For example mcreate=mdbm_create, mfetch=mdbm_fetch,...
 * It also implements several new commands: mstore, mdelete, mopen, mclose, mlock_print,
 * and mlock_reset.
 * Mash also supports a limited version of the cd, ls, pwd, and cat Unix commands.
 * Typing "help" or "?" will display the list of available commands.
 * "quit" or "exit" gets you out of MASH and into the Unix shell.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <regex.h>
#include <time.h>
#include <pwd.h>
#include <grp.h>
#include <ctype.h>
#include <limits.h>

#include <openssl/md5.h>
#include <openssl/sha.h>

#include <string>
#include <vector>
#include <set>
#include <map>

#include <dirent.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "mdbm.h"
#include "mdbm_util.h"
#include "mdbm_internal.h"

extern "C" {
extern void print_lock_state_inner(struct mdbm_locks *locks);
extern struct mdbm_locks* open_locks_inner(const char *dbname, int flags, int doLock,
                                           int *needCheck);
extern void close_locks_inner(struct mdbm_locks* locks);
}

using namespace std;

class MashCommand;
typedef map<string, MashCommand *>::iterator CommandIterator;

static const char* WHITESPACE = " \t\f\n\v";
static string PwdStr("/");
static bool DoneProcessing = false;   // DoneProcessing=true means Exit now
// Code in main() and the readline library needs use OutputFilePtr/ErrorFilePtr so that
// output/error messages go to the right output location, which may be a file.
static FILE *OutputFilePtr = stdout;
static FILE *ErrorFilePtr = stderr;

// Constant defining how to reference the current working MDBM
static const string CurrentWorkingSymbol("%_");

// returns true if a string contains a wildcard symbol
static inline bool IsWildcard(const string& arg)
{
    return (arg.find_first_of("*?[]") != string::npos);
}

// Determines if a string is an option
static inline bool IsOption(const string& arg)
{
    return (arg[0] == '-');
}

static inline bool GetParentDirectory(const string &path, string &parent)
{
    string::size_type sep = path.find_last_of("/");
    if ((sep == string::npos) || (sep == 0)) {
        return false;
    }
    parent = path.substr(0, sep);
    return true;
}

// Determines if a string is an option, and sets bool longOption to true if
// there are 2 minus signs, which signifies it is a long option
static inline bool IsOptionGetType(const string& arg, bool &longOption)
{
    longOption = false;

    // a long option must be 2 minus signs followed by at least 1 character
    if ((arg.size() > 2) && (arg.substr(0,2) == string("--"))) {
        longOption = true;
    }

    return (arg[0] == '-');
}

// Removes dashes in front of option names, therefore returning the option name itself
static inline string GetOptionName(const string& arg)
{
    size_t pos = arg.find_first_not_of('-');
    if (pos > 2) {
        return string("");
    }
    return arg.substr(pos);
}

/**
 * Print a datum (an MDBM key or value) in a format that resembles the "xxd" Linux util
 *   \param[in] outfile - output xxd-like formatted data to this FILE pointer.
 *   \param[in] prefix  - print this character string before printing formatted data
 *   \param[in] d       - MDBM datum (key or value)
 */
void PrintDatum(FILE *outfile, const char *prefix, datum d)
{
    int i, len;
    char *s;

    len = d.dsize;
    s = d.dptr;
    while (len > 0) {
        fprintf(outfile, "%s%p  ", prefix, s);
        for (i = 0; i < 16; i++) {
            if (i == 8) {
                fputc(' ', outfile);
            }
            if (i < len) {
                fprintf(outfile, "%02x ", static_cast<unsigned char>(s[i]));
            } else {
                fputs("   ", outfile);
            }
        }
        fputs("  ", outfile);
        for (i = 0; i < 16; i++) {
            if (i < len) {
                if (s[i] > ' ' && s[i] < 0x7f) {
                    fputc(s[i], outfile);
                } else {
                    fputc('.', outfile);
                }
            }
        }
        s += 16;
        len -= 16;
        fputc('\n', outfile);
    }
}

/**
 * class MashUtils: singleton class of various methods used by mash to process text
 */

class MashUtils
{
public:
    // Trim whitespace from the start and end of 'text'.
    static void Trim(string &text)
    {
        string::size_type s = text.find_first_not_of(WHITESPACE);
        string::size_type e = text.find_last_not_of(WHITESPACE);

        if (s==string::npos || e==string::npos) {
            text.clear();
        } else {
            text = text.substr(s, e+1-s);
        }
    }

    // Return the fully qualified, absolute path.
    // If a relative path is provided, it is converted to an absolute path.
    // The "~" character signifying a user's home directory is not supported.
    static const string FullPath(const string &path)
    {
        string fullPath(path);
        // For relative path, use the Mash-internal relative path
        if (path.size() && path[0] != '/') {
            if (PwdStr == string("/")) {
                fullPath = PwdStr + fullPath;
            } else {
                fullPath = PwdStr + string("/") + fullPath;
            }
        }
        return fullPath;
    }

    // split a string into tokens, separating them using the whitespace between them.
    static int SplitTokens(string &cmd, vector<string> &tokens)
    {
        string::size_type s = 0;
        string::size_type e = cmd.find_first_of(WHITESPACE, s);

        tokens.clear();
        while ((e != string::npos)) {
            string tmp = cmd.substr(s, e-s);
            tokens.push_back(tmp);
            s = cmd.find_first_not_of(WHITESPACE, e);
            if (s == string::npos) {
                break;
            }
            e = cmd.find_first_of(WHITESPACE, s);
        }
        if (s!=string::npos) {
            string tmp = cmd.substr(s, cmd.size()+1-s);
          tokens.push_back(tmp);
        }
        return tokens.size();
    }

    /**
     * MergePath() takes a directory and a subdirectory (e.g. to chdir to) and merges them
     *  \param[in,out] dst - A string into which to store the merged directory (destination)
     *  \param[in]     sub - A subdirectory
     *  \return false if failed, true if successful
     */
    static bool MergePath(string &dst, const string &sub)
    {
        string::size_type found;
        string newDir = dst;

        if (sub.size() && '/'==sub[0]) {
            newDir = sub;
        } else {
            // ensure a slash between directories
            if ('/' != newDir[newDir.size()-1]) {
                newDir += '/';
            }
            newDir += sub;
        }
        // ensure trailing slash
        if ('/' != newDir[newDir.size()-1]) {
            newDir += '/';
        }

        // simplify any parent-relative paths
        while (string::npos != (found = newDir.find("/../")) ) {
            string::size_type start = newDir.rfind('/', found-1);
            if (start==string::npos) {
                fprintf(ErrorFilePtr, "Cannot change to non existent directory [%s] from [%s]\n",
                    sub.c_str(), dst.c_str());
                return false;
            } else if (0 == found && 3 == start) {
                newDir.erase(1, 3);
            } else {
                newDir.erase(start, (found+3-start));
            }
        }

        // remove any no-op paths
        while (string::npos != (found=newDir.find("/./")) ) {
            newDir.erase(found, 2);
        }
        while (string::npos != (found=newDir.find("//")) ) {
            newDir.erase(found, 1);
        }

        dst = newDir;
        return true;
    }

    /**
     * FinalizePath() takes a directory's relative path, merges it with the current working dir,
     * so that we get a fully qualified directory path.
     *  \param[in,out] path - A string into which to store the fully qualified directory path
     *  \return false if failed, true if successful
     */
    static bool FinalizePath(string& path)
    {
        if (path.size()==0 || path[0]!='/') {
            string tmp = PwdStr;
            if (!MergePath(tmp, path)) {
                return false;
            }
            path = tmp;
        }
        if (path.size()<2) {
            return true;
        }
        if ('/' == path[path.size()-1]) {
            path.resize(path.size()-1);
        }
        return true;
    }

    /**
     * Returns the directory listing of a given path
     *  \param[in]  path    - directory for which to produce the listing
     *  \param[out] entries - the directory listing, excluding "." and ".."
     *  \return  true if successfully produced a listing, false if not.
     */
    static bool GetDirectoryListing(const string& path, vector<string> &entries)
    {
        if (path.size() == 0) {
            return false;
        }
        DIR *dir;
        struct dirent *ent;
        if (!(dir = opendir (path.c_str()))) {
            fprintf(ErrorFilePtr, "%s: Invalid directory\n", path.c_str());
            return false;
        }
        /* print all the files and directories within directory */
        while ((ent = readdir (dir)) != NULL) {
            string name(ent->d_name);
            if ((name != string(".")) && (name != string(".."))) {
                //printf ("%s\n", ent->d_name);
                entries.push_back(name);
            }
        }
        closedir (dir);
        return true;
    }

    // assumes the wildcard is at the end of the string
    // doesn't do recursive matching
    // TODO just split by directory separator '/' and expand into a tree...
    //   this doesn't allow for wildcard parts that span dir/subdir, but is much better
    static int GlobWildcardTail(const string& pattern, vector<string> &matched)
    {
        string parent;
        regex_t regex;
        regmatch_t match;
        vector<string> entries;

        // force full line match?
        //fullpat = "^"; fullpat += pattern; fullpat += "$";
        if (regcomp(&regex, pattern.c_str(), REG_EXTENDED|REG_NOSUB)) {
            fprintf(ErrorFilePtr, "REGEX compiling %s FAILED \n", pattern.c_str());
          return -1;
        }
        GetParentDirectory(pattern, parent);
        GetDirectoryListing(parent, entries);
        for (int i=entries.size()-1; i>=0; --i) {
            string current = parent; current += '/'; current += entries[i];
            if (0==regexec(&regex, current.c_str(), 1, &match, 0)) {
                matched.push_back(current);
            }
        }
        regfree(&regex);
        return 0;
    }
};

MashUtils Utils;

// Store MDBM handle information in the following structure:
typedef struct {
    MDBM *handle;    //  The MDBM handle
    int  refCount;   //  The handle's reference count: we need to close equal to zero
    int  lockCount;  //  Keeps track of the number of times this MDBM was locked
} MdbmHandleInfo;

/**
 * MdbmState: Singleton storage class and methods to maintain and manipulate
 * the state information for all open MDBM handles.
 */
class MdbmState
{
public:

    MdbmState()
    {
    }

    // Get the current (last handle used) MDBM handle.
    // Find handle by looking up the handle belonging to the current MDBM file name.
    // Return handle, or NULL if not found.
    virtual MDBM *GetCurrentMdbm()
    {
        if (CurrentMdbmName.size() == 0) {
            return NULL;
        }
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(CurrentMdbmName);
        if (it != MdbmFileHandles.end()) {
            return it->second.handle;
        }
        return NULL;
    }

    // Using a fully qualified MDBM name, find its handle.
    // Return handle, or NULL if not found.
    MDBM *FindHandle(const string &fullPath)
    {
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(fullPath);
        if (it != MdbmFileHandles.end()) {
            return it->second.handle;
        }
        return NULL;
    }

    // Returns whether handle has been found and successfully marked as unlocked.
    // Will not call mdbm_unlock_smart()
    bool UnlockHandle(const string &fullPath)
    {
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(fullPath);
        if (it != MdbmFileHandles.end() && (it->second.lockCount > 0)) {
            --(it->second.lockCount);
            return true;
        }
        return false;
    }

    // Take an MDBM handle and store it, or if found, increment the reference count of
    // the MDBM handle associated with the name
    virtual void StoreHandle(MDBM *db)
    {
        if (db == NULL) {
            return;
        }
        string name(mdbm_get_filename(db));
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(name);
        if (it == MdbmFileHandles.end()) {
            MdbmHandleInfo newEntry;
            newEntry.handle = db;
            newEntry.refCount = 1;
            newEntry.lockCount = 0;
            MdbmFileHandles[name] = newEntry;
        } else {   // If found, just increment ref count
            //printf("StoreHandle: refCount=%d\n",  ++(it->second.refCount));
            ++(it->second.refCount);
        }
    }

    /**
     * Drop name and handle belonging to an MDBM
     *
     * Bump down reference count, then delete handle, but only if reference count is zero.
     * Look up MDBM using name.
     *  \param[in] name - fully qualified path of an MDBM
     *  \param[in] if ignoreRefCount is true, always close, drop name and handle.
     *             if false, bump down ref-count and drop only if reference count is zero.
     */
    virtual void DropName(const string &name, bool ignoreRefCount = false)
    {
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(name);
        if (it != MdbmFileHandles.end()) {
            int lockCount = it->second.lockCount;
            int refCount = --(it->second.refCount);
            if (ignoreRefCount || ((lockCount == 0) && (refCount <= 0))) {
                // Close locks and print message that we unlocked on-close
                int locksClosed = UnlockLocks(it->second.handle);
                if (locksClosed) {
                    fprintf(OutputFilePtr, "Unlocked %d lock(s) when closed\n", locksClosed);
                }
                ReleaseHandle(it->second.handle);
                MdbmFileHandles.erase(it);
            }
        }
    }

    // Identical to DropName(filename) above, but just use the MDBM handle as a parameter.
    // Bump down reference count, then drop the name and delete handle, but only
    // if reference count is zero, because DropName() is called w/o ignoreRefCount=false
    virtual void DropHandle(MDBM *db)   // Look up using MDBM handle
    {
        if (db == NULL) {
            return;
        }
        string name(mdbm_get_filename(db));
        DropName(name);
    }

    // Lock an MDBM handle:
    // Store handle if needed, then set lockCount to 1 or increment lockCount
    virtual void LockHandle(MDBM *db)
    {
        if (db == NULL) {
            return;
        }
        string name(mdbm_get_filename(db));
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(name);
        if (it == MdbmFileHandles.end()) {
            MdbmHandleInfo newEntry;
            newEntry.handle = db;
            newEntry.refCount = newEntry.lockCount = 1;
            MdbmFileHandles[name] = newEntry;
        } else {   // If found, just increment ref count
            ++(it->second.lockCount);
        }
    }

    // If an MDBM handle is currently open, return its name.  Otherwise:
    // get the current/last MDBM File name that was in use
    virtual string GetCurrentMdbmFile()
    {
        MDBM *currentHandle = GetCurrentMdbm();
        if (currentHandle) {
            return mdbm_get_filename(currentHandle);
        }

        return CurrentMdbmName;
    }

    // Record the current MDBM file name that was just used
    virtual void SetCurrentMdbmFile(const string &path)
    {
        CurrentMdbmName = path;
    }

    // For all open and possibly locked MDBMs stored by MdbmState class:
    //   unlocks if necessary, and close the MDBM. Used to clean up upon exit.
    virtual void CloseOpenMdbms()
    {
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.begin();
        for (;it != MdbmFileHandles.end(); ++it) {
            ReleaseHandle(it->second.handle);
        }
        MdbmFileHandles.clear();
        CurrentMdbmName = "";
    }

    // When we perform "file-level" operations, we should not have the file locked,
    // because on many occasions underlying APIs lock the MDBM.
    // So we check if "fullPath" is open and locked.  If locked we unlock it.
    virtual void CheckUnlockLockedMdbm(const string &fullPath)
    {
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(fullPath);
        if (it != MdbmFileHandles.end()) {
            int lockCount = it->second.lockCount;
            MDBM *db = it->second.handle;
            if (db && (lockCount > 0)) {
                fprintf(OutputFilePtr, "Unlocking locked MDBM %s\n", fullPath.c_str());
                for (int i = 0; i < lockCount; ++i) {
                    mdbm_unlock_smart(db, NULL, 0);
                }
            }
        }
    }

protected:

    // ReleaseHandle() unlocks all locks, then closed the MDBM
    virtual void ReleaseHandle(MDBM *db)
    {
        if (db != NULL) {
            string name(mdbm_get_filename(db));
            if (name == CurrentMdbmName) {
                CurrentMdbmName = "";
            }
            UnlockLocks(db);
            mdbm_close(db);
        }
    }

    // For a given MDBM handle, find its lockCount, then unlock the MDBM for each time indicated
    // by lockCount, and reset the lockCount.
    //  \return original(pre-unlocking) lockCount
    virtual int UnlockLocks(MDBM *db)
    {
        string name(mdbm_get_filename(db));
        int lockCount = 0;
        map<string, MdbmHandleInfo>::iterator it = MdbmFileHandles.find(name);
        if (it != MdbmFileHandles.end()) {
            lockCount = it->second.lockCount;
        }

        for (int i = 0; i < lockCount; ++i) {
            mdbm_unlock_smart(db, NULL, 0);
        }
        it->second.lockCount = 0;
        return lockCount;
    }

private:
    map<string, MdbmHandleInfo> MdbmFileHandles;   // Store of MDBM handle information
    string CurrentMdbmName;                        // Name of MDBM currently being accessed
};

MdbmState MdbmData;

/**
 * Base class for handling generic MASH commands, including MDBM and non-MDBM related commands.
 *  Unix-like MASH commands derive from MashCommand.
 *  MDBM commands derive from a subclass of MashCommand called MdbmCommand.
 */
class MashCommand
{
public:
    static map<string, MashCommand*> commands;
public:
    string cmdName;
    string desc;
public:
    MashCommand() : cmdName("<unknown>"), desc("<unknown>") {}
    MashCommand(const char* name_, const char* desc_) : cmdName(name_), desc(desc_) {}
    virtual ~MashCommand() {}

    // Register a command in internal storage, so that we can do completion and help
    static int RegisterCommand(MashCommand* cmd) {
        commands[cmd->cmdName] = cmd;
        return commands.size();
    }

    // return a MashCommand from internal storage, or NULL if not found
    static MashCommand* FindCommand(const char* name)
    {
        CommandIterator found = commands.find(name);
        if (found != commands.end()) {
            return found->second;
        }
        return ((MashCommand *)NULL);
    }

    // Convert a path, which can be relative to "pwd", into a fully qualified path.
    virtual string GetFullPath(const string &path)
    {
        return Utils.FullPath(path);
    }

    virtual bool DoFullPaths() { return true; }
    virtual bool DoGlobPaths() { return true; }

    virtual bool WarnNoGlobMatch() { return false; }
    virtual bool ErrorNoGlobMatch() { return false; }

    /**
     * Expand the arguments, handling wildcards and relative paths.
     * Use to expand the path argument(s) of the "ls" command.
     * TODO: fix handling of path wildcards (currently generating: "REGEX compiling FAILED")
     *  \param[in]  argsIn  - arguments to expand
     *  \param[out] argsOut - expanded arguments
     *  \return true if successful, false if not
     */
    virtual bool ExpandArgs(const vector<string> &argsIn, vector<string> &argsOut)
    {
        if (!DoFullPaths() && !DoGlobPaths()) {
            argsOut = argsIn;
            return true;
        }
        // push the command name
        argsOut.push_back(argsIn[0]);
        for (unsigned i=1; i<argsIn.size(); ++i) {
            const string &word=argsIn[i];
            if (IsOption(word)) {
                argsOut.push_back(word);
            } else if (DoGlobPaths() && IsWildcard(word)) {
                vector<string> entries;
                Utils.GlobWildcardTail(word, entries);
                if (entries.size() == 0) {
                    if (ErrorNoGlobMatch()) {
                        fprintf(ErrorFilePtr, "ERROR: %s No Match %s\n", argsIn[0].c_str(),
                                word.c_str());
                        return -1;
                    } else if (WarnNoGlobMatch()) {
                        fprintf(ErrorFilePtr, "Warn: %s No Match %s\n",
                                argsIn[0].c_str(), word.c_str());
                    }
                }
                for (unsigned c=0; c<entries.size(); ++c) {
                    if (DoFullPaths()) {
                        string path(PwdStr);
                        Utils.MergePath(path, entries[i]);
                        // strip trailing dir-slash
                        if (path.size()>1) {
                            path=path.substr(0, path.size()-1);
                        }
                        argsOut.push_back(path);
                    } else {
                        argsOut.push_back(entries[i]);
                    }
                }  // for
            } else {
                string path(PwdStr);
                Utils.MergePath(path, word);
                // strip trailing dir-slash
                if (path.size()>1) {
                    path=path.substr(0, path.size()-1);
                }
                argsOut.push_back(path);
            }
        }

        return true;
    }

    // Run the command, with a vector of string args
    // args[0] contains the command name
    virtual int Run(const vector<string> &args)
    {
        if (args.size()==1) {
            // call the function with no args
            return (Run());
        } else {
            unsigned i;
            // loop over args, so commands don't have to deal with args
            // TODO do absolute path conversion here...
            for (i=1; i<args.size(); ++i) {
                const string &word=args[i];
                if (IsWildcard(word)) {
                    vector<string> entries;
                    // put the command itself in...
                    entries.push_back(args[0]);
                    Utils.GlobWildcardTail(word, entries);
                    if (entries.size() > 1) {
                        int rc = Run(entries);
                        if (rc) {
                            return rc;
                        }
                     } else {
                         fprintf(ErrorFilePtr,
                                 "%s No Matches: %s\n", args[0].c_str(), word.c_str());
                     }
                } else {
                    // Call the function.
                    int rc = (Run(word));
                    if (rc) {
                        return rc;
                    }
                }
            } // for
        }
        return (0);
    }

    // Run a Mash command w/o args
    virtual int Run()
    {
        fprintf(ErrorFilePtr, "command %s cannot run without an argument\n", cmdName.c_str());
        return -1;
    }

    // Run a Mash command with one arg
    virtual int Run(const string& arg)
    {
        fprintf(ErrorFilePtr, "command %s has not been implemented\n", cmdName.c_str());
        return -1;
    }
};

map<string, MashCommand*> MashCommand::commands;

// Macro for easily registering a command.
// Define a dummy variable to store each command entry
#define REGISTER_MASH_COMMAND(name) \
    name name##Prototype;           \
    int name##Dummy = MashCommand::RegisterCommand(&name##Prototype);


///////////////////////////////////////////////////////////////////
// Basic commands (help, quit/exit)
///////////////////////////////////////////////////////////////////

// The ? (same as help) command
class HelpCommand : public MashCommand
{
public:
    HelpCommand() : MashCommand("?", "Show Help") {}
    HelpCommand(const char* name, const char* desc) : MashCommand(name, desc) {}
    virtual int Run()
    {
        CommandIterator iter;
        for (iter=commands.begin(); iter!=commands.end(); ++iter) {
            fprintf(OutputFilePtr, "%s\t\t%s.\n", iter->first.c_str(), iter->second->desc.c_str());
        }
        return (0);
    }

    virtual int Run(const string& arg)
    {
        int printed = 0;

        CommandIterator iter;
        for (iter=commands.begin(); iter!=commands.end(); ++iter) {
            if (iter->first == arg) {
                fprintf(OutputFilePtr,
                        "%s\t\t%s.\n", iter->first.c_str(), iter->second->desc.c_str());
                printed++;
            }
        }

        if (!printed) {
            fprintf(OutputFilePtr, "No commands match '%s'\n", arg.c_str());
             Run();
        }
        return (0);
    }
};

// Register "?" to show top-level help
REGISTER_MASH_COMMAND(HelpCommand)

// Register "help", which is the same as typing "?", to show top-level help
HelpCommand HelpCommandPrototype2("help", "Show Help");
int HelpDummy2 = MashCommand::RegisterCommand(&HelpCommandPrototype2);

// Exit command: to exit "mash".
class ExitCommand : public MashCommand
{
public:
    // Constructor requires that you explicitly specify the command name (exit or quit).
    ExitCommand(const char* name, const char* helpMsg) : MashCommand(name, helpMsg) {}
    virtual int Run()
    {
        DoneProcessing = true;
        return (0);
    }
};

// Register "exit" to exit mash.
ExitCommand ExitCommandPrototype("exit", "Exit the program");
int ExitDummy = MashCommand::RegisterCommand(&ExitCommandPrototype);
// Register "quit" to exit mash.
ExitCommand ExitCommandPrototype2("quit", "Quit the program");
int QuitDummy = MashCommand::RegisterCommand(&ExitCommandPrototype2);


///////////////////////////////////////////////////////////////////

class SleepCommand : public MashCommand
{
public:
    SleepCommand() : MashCommand("sleep", "sleep for N (default 1) seconds") {}
    SleepCommand(const char* name, const char* desc) : MashCommand(name, desc) {}
    virtual int Run()
    {
        sleep(1);
        return (0);
    }

    virtual int Run(const string& arg)
    {
        int sec = strtoul(arg.c_str(), NULL, 0);
        sleep(sec);
        return (0);
    }
};

// Register "?" to show top-level help
REGISTER_MASH_COMMAND(SleepCommand)


///////////////////////////////////////////////////////////////////
//
///////////////////////////////////////////////////////////////////

// Execute a command line.
int execute_line(string &line)
{
    vector<string> tokens;
    MashCommand *command;
    const char *cmd;

    Utils.SplitTokens(line, tokens);
    if (!tokens.size()) {
        return (1);
    }

    cmd = tokens[0].c_str();
    command = MashCommand::FindCommand(cmd);

    if (!command) {
        fprintf(ErrorFilePtr, "%s: No such command.\n", cmd);
        return (-1);
    }

    return command->Run(tokens);
}


///////////////////////////////////////////////////////////////////
//                  Readline Completion Section
///////////////////////////////////////////////////////////////////

// We use the readline library to support path completion

// command completion.
char* command_generator(const char* text, int state)
{
    static CommandIterator iter = MashCommand::commands.end();
    static int len;

    if (!state) { // first call for this completion, initialize state
        iter = MashCommand::commands.begin();
        len = strlen(text);
    }
    // Return next partial match
    while (iter!=MashCommand::commands.end()) {
        const string &name = iter->first;
        ++iter;
        if (strncmp(name.c_str(), text, len) == 0) {
            return (strdup(name.c_str()));
        }
    }
    // If no names matched, then return NULL.
    return (NULL);
}

// Returns a malloc-allocated string representing the longest
// common leading characters of the strings in list.
// If there are none in common, it still returns an allocated
// 1-byte string "", containing the NUL character.
char* longestCommonPrefix(char* list[])
{
    int curLen, len = 0;
    char** cur;
    char* str = NULL;
    const char* prefix = "";

    if (list && list[0]) {
        prefix = list[0];
        len = strlen(prefix);
        for (cur=list+1; cur && cur[0]; ++cur) {
            curLen = strlen(*cur);
            if (curLen>len) {
                curLen=len;
            }
            for (len=0; len<curLen && (prefix[len]==(*cur)[len]); ++len)
                ;
            if (!len) {
                break;
            }
        }
    }
    str = strndup(prefix, len);
    return str;
}

// Attempt to complete 'text', between 'start' and 'end' in rl_line_buffer.
// Return the array of matches, or NULL if there aren't any.
char** mash_completion(const char* text, int start, int end)
{
    (void)end; // make pedantic compilers happy
    char **matches = (char **)NULL;

    // completions on the local filesystem are not useful...
    rl_filename_completion_desired = 0;

    if (start == 0) {
        // at start of the line, do command completion
      matches = rl_completion_matches(text, command_generator);
    } else {
        //filename completion
        string sub(text);
        string full, part;
        string path=PwdStr;

        string::size_type found = sub.rfind('/');
        if (string::npos != found) {
            full = sub.substr(0, found);
            part = sub.substr(found+1);
        } else {
            part = sub;
        }

        if (!Utils.MergePath(path, full)) {
            //printf("Merge failed, returning NULL\n");
            return (NULL);
        }
        int plen = path.size();
        if (plen>1 && '/'==path[plen-1]) {
            path.erase(plen-1);
        }

        int j=0;
        vector<string> list;
        bool rc = Utils.GetDirectoryListing(path.c_str(), list);
        if (rc && list.size()) {
            matches = (char**)malloc((list.size()+2) * sizeof(char*));
            matches[0] = NULL;
            ++j;
            for (unsigned int i=0; i<list.size(); ++i) {
                if (!strncmp(part.c_str(), list[i].c_str(), part.size())) {
                    string filename(path);
                    string cur(list[i]);
                    Utils.MergePath(filename, cur);
                    matches[j++] = strdup(filename.c_str());
                }
            }
            if (j>1) {
                matches[j] = NULL;
                matches[0] = longestCommonPrefix(matches+1);
            } else {
                free(matches[0]);
                free(matches);
                matches=NULL;
            }
        }
    }
    if (matches) {
        rl_completion_suppress_append = 1;
    }
    return (matches);
}

void initialize_readline()
{
    // Allow conditional parsing of the ~/.inputrc file.
    rl_readline_name = "mash";
    // Tell the completer that we want a crack first.
    rl_attempted_completion_function = mash_completion;
}


////////////////////////////////////////////////////////////////////////////////
// Classes that implement Unix commands from inside Mash
////////////////////////////////////////////////////////////////////////////////

// ListState is a class that stores the state returned by parsing the command line args
class ListState
{
public:
    bool full;    // -f show full path
    bool stat;    // -l show ctime/mtime/version/size
    bool perm;    // -Z show perms and owner/group

public:
    ListState() { Reset(); }
    void Reset()
    {
        full=false;
        stat=false;
        perm=false;
    }

    int ParseArg(const string& arg)
    {
        for (unsigned j=1; j<arg.size(); ++j) {
            switch (arg[j]) {
                case 'f' : full = true;    break;
                case 'l' : stat = true;   break;
                case 'Z' : perm = true;     break;
                default:
                    fprintf(ErrorFilePtr, "invalid option [%s]\n", arg.c_str());
                    return -1;
            }
        }
        return 0;
    }
};

// ListFile actually does most of the work of performing the "ls" mash command

int ListFile(const string& pref, const string& path0, const ListState& state)
{
    string path = path0;
    Utils.FinalizePath(path);
    vector<string> list;

    bool rc = Utils.GetDirectoryListing(path.c_str(), list);
    if (rc) {
        for (unsigned int i=0; i<list.size(); ++i) {
            string sub = path;
            Utils.MergePath(sub, list[i]);
            Utils.FinalizePath(sub);
            string subPref = pref;
            string stats;
            string fullPath = pref + sub;  // sub has a trailing '/' ...
            bool gotStat = false;
            struct stat statBuf;

            if (state.stat || state.perm) {  // print permissions and owner/group
                if (stat(fullPath.c_str(), &statBuf) == 0) {
                    gotStat = true;
                    char permis[64];
                    snprintf(permis, sizeof(permis), "%o\t", statBuf.st_mode);
                    stats += permis;
                    struct passwd *pass;
                    if ((pass = getpwuid(statBuf.st_uid)) != NULL) {
                        stats += pass->pw_name;
                    }
                    struct group *grp;
                    if ((grp = getgrgid(statBuf.st_gid)) != NULL) {
                        stats += string("\t") + grp->gr_name + string("\t");
                    }
                }
            }

            if (gotStat && state.stat) {
                // print out size/date
                char siz[64];
                snprintf(siz, sizeof(siz), "%llu\t", (unsigned long long) statBuf.st_size);
                stats += siz;
                stats += ctime(&statBuf.st_mtime);
                stats[stats.size()-1] = ' ';
            }
            if (stats.size()) {
                fprintf(OutputFilePtr, "%s ", stats.c_str());
            }
            if (state.full) {
                fprintf(OutputFilePtr, "%s\n", fullPath.c_str());
            } else {
                fprintf(OutputFilePtr, "%s%s\n", pref.c_str(), list[i].c_str());
                //subPref += "  ";
            }
        }
    }

    return rc;
}

// ListCommand is a subclass of MashCommand that implements the various Run() interfaces
// of MashCommand to allow "ls" to work with 0,1 or more args.

class ListCommand : public MashCommand
{
public:
    ListCommand() : MashCommand("ls", "List files/dirs in current directory") {}
    virtual int Run()
    {
        string dummy;
        return Run(dummy);
    }

    virtual int Run(const vector<string> &args)
    {
        state.Reset();
        vector<string> fileArgs;
        for (unsigned i=0; i<args.size(); ++i) {
            const string& cur = args[i];
            if (cur.size() && IsOption(cur)) {
                if (state.ParseArg(cur)) {
                    return -1;
                }
            } else {
                fileArgs.push_back(cur);
            }
        }
        vector<string> expandedArgs;
        if (ExpandArgs(fileArgs, expandedArgs)) {
            return MashCommand::Run(expandedArgs);
        } else {
            return MashCommand::Run(fileArgs);
        }
    }

    virtual int Run(const string& arg)
     {
        return ListFile("", arg, state);
     }

public:
    ListState state;
};

// Register ListCommand
REGISTER_MASH_COMMAND(ListCommand)

// CatCommand implements the "cat" mash command.
// No command-line args supported, so no state class needed
class CatCommand : public MashCommand
{
public:
    CatCommand() : MashCommand("cat", "Show file contents") {}
    virtual int Run(const string &onearg)
    {
        string arg = onearg;
        Utils.FinalizePath(arg);
        const int bufLen = 65536;
        char buf[bufLen];
        int fd = open(arg.c_str(), O_RDONLY);
        if (fd < 0) {
            fprintf(OutputFilePtr, "Cat could not open [%s], %s\n", arg.c_str(), strerror(errno));
            return (1);
        }
        int bytes = pread(fd, buf, bufLen-1, 0);
        fprintf(OutputFilePtr, "[%s] %d bytes: \n", arg.c_str(), bytes);
        if (bytes<=0) {
            fprintf(OutputFilePtr, "<empty>\n");
        } else {
            bool truncated = false;
            if (bytes>bufLen) {
                bytes = bufLen;
                truncated = true;
            }
            buf[bytes]='\0';
            fprintf(OutputFilePtr, "%s\n", buf);
            if (truncated) {
                fprintf(OutputFilePtr, "<truncated>\n");
            }
        }
        close(fd);
        return (0);
    }
};

// Register CatCommand
REGISTER_MASH_COMMAND(CatCommand)

// "cd" supports only 1 arg an no options.
// It updates PwdStr and prints the directory it chdir-ed to
class CdCommand : public MashCommand
{
public:
    CdCommand() : MashCommand("cd", "Change working directory") {}
    virtual int Run(const string& onearg)
    {
        string arg = onearg;
        if (!Utils.FinalizePath(arg)) {
            fprintf(ErrorFilePtr, "Cannot change directory: %s invalid\n", onearg.c_str());
            return -1;
        }

        struct stat statBuf;
        if (stat(arg.c_str(), &statBuf) != 0) {
            fprintf(ErrorFilePtr, "Directory %s does not exist\n", arg.c_str());
            return -1;
        } else if (!S_ISDIR(statBuf.st_mode)) {
            fprintf(ErrorFilePtr, "%s is not a directory\n", onearg.c_str());
            return -1;
        }
        PwdStr=arg;
        fprintf(OutputFilePtr, "[%s]\n", PwdStr.c_str());
        return (0);
    }
};

// Register the "cd" Command
REGISTER_MASH_COMMAND(CdCommand)

// Print the current working directory
class PwdCommand : public MashCommand
{
public:
    PwdCommand() : MashCommand("pwd", "Print working directory") {}
    virtual int Run()
    {
        fprintf(OutputFilePtr, "[%s]\n", PwdStr.c_str());
        return 0;
    }
};

// Register the "pwd" command
REGISTER_MASH_COMMAND(PwdCommand)


// MDBM Option processing functions, classes and their methods

// Option types supported by the MDBM command-line options processor
enum OptionType
{
    OPTION_TYPE_STRING   = 0, // string
    OPTION_TYPE_FLAG     = 1, // boolean on/off
    OPTION_TYPE_UNSIGNED = 2, // 64-bit Unsigned (parse with strtoull)
    OPTION_TYPE_SIGNED   = 3, // 64-bit Signed (parse with strtoll)
    OPTION_TYPE_FLOAT    = 4, // double-precision float (parse with strtod)
    OPTION_TYPE_SIZE     = 5  // 64-bit Unsigned size shortcut, with suffixes of:
                              // k=1024 (Kbyte), m=1024*1024 (Mbyte) and g=1024*1024*1024 (GByte)
};

// Pretty-print the type name
string getTypeName(OptionType opt)
{
    static const char *OPTION_TYPE_NAMES[] = {
        "String", "Flag", "Unsigned", "Signed", "Floating point", "Size" };
    string type;

    int optIdx = opt;

    if (optIdx > OPTION_TYPE_SIZE) {
        type = "Unknown";
    } else {
        type = OPTION_TYPE_NAMES[optIdx];
    }
    return type;
}

// Useful parsing/extraction methods
class ParseUtils
{

protected:

     // convenience wrapper to generate error messages
    void
    generateErrMsg(const string &stringArg, const string &opt, OptionType type, string &errorMsg)
    {
        static const int MAX_VALUE_LEN = 80;
        errorMsg = string("Invalid value: ") + stringArg.substr(0, MAX_VALUE_LEN);
        errorMsg += string(", after option -") + opt + string(", expecting ") + getTypeName(type);
    }

public:

    // Extract unsigned int option-value after validating it
    bool ExtractUint64(const string &stringArg, const string &opt, uint64_t &val, string &errorMsg)
    {
        char *ends;
        val = strtoull(stringArg.c_str(), &ends, 0);
        if (ends[0]) {
            generateErrMsg(stringArg, opt, OPTION_TYPE_UNSIGNED, errorMsg);
            return false;
        }
        return true;
    }

    // Extract signed option-value after validating it
    bool ExtractInt64(const string &stringArg, const string &opt, int64_t &val, string &errorMsg)
    {
        char *ends;
        val = strtoll(stringArg.c_str(), &ends, 0);
        if (ends[0]) {
            generateErrMsg(stringArg, opt, OPTION_TYPE_SIGNED, errorMsg);
            return false;
        }
        return true;
    }

    // Extract floating point option-value after validating it
    bool ExtractFloat(const string &stringArg, const string &opt, double &val, string &errorMsg)
    {
        char *ends;
        val = strtod(stringArg.c_str(), &ends);
        if (ends[0]) {
            generateErrMsg(stringArg, opt, OPTION_TYPE_FLOAT, errorMsg);
            return false;
        }
        return true;
    }

    // Extract a size: which can be an unsigned int followed by an optional k/m/g
    bool ExtractSize(const string &stringArg, const string &opt, uint64_t &val, string &errorMsg)
    {
        if (!isdigit(stringArg[0])) {
            generateErrMsg(stringArg, opt, OPTION_TYPE_SIZE, errorMsg);
            return false;
        }
        char *ends;
        val = strtoull(stringArg.c_str(), &ends, 0);
        if (ends[0]) {
            char sfx = tolower(ends[0]);
            switch (sfx) {
                case 'g':
                    val *= 1024;   // No break on purpose
                case 'm':
                    val *= 1024;   // No break on purpose
                case 'k':
                    val *= 1024;
                    break;
                default:
                    generateErrMsg(stringArg, opt, OPTION_TYPE_SIZE, errorMsg);
                    return false;
            }
        }
        return true;
    }
};

ParseUtils Extractor;

// Storage structure for storing information about a single MDBM-related command-line option
struct OptionDescriptor
{
    string     option; // short option name, i.e. "-s"
    OptionType type;   // type, as above
    string     label;  // uniform long name, i.e. "pagesize", "lockmode"
    string     longDescription;  // long description of what this option does (used by help).
};

// Container class: a repository of parsed options, and extra arguments
class OptionsSet
{
public:
    map<string, string> options;   // parsed and type-validated options: indexed by option name
    map<string, OptionType> types; // type of each option: indexed by option name
    vector<string> extra;          // extra non-option arguments (i.e. list of filenames)

public:

    // In all the Get* and Set methods below, "label" is the long-option name,
    // and "opt" is the short (single-letter) option name

    // get the option as a particular type, put into 'val', returns false if not present
    // Retrieve a string-valued option.
    bool GetOption(const string &label, string &val)
    {
        map<string, string>::iterator it = options.find(label);
        if (it == options.end()) {
            return false;
        }
        val = it->second;
        return true;
    }

    // Retrieve a flag/boolean command-line arg value.
    bool GetOption(const string &label, bool &val)
    {
        map<string, string>::iterator it = options.find(label);
        if (it == options.end()) {
            return false;
        }
        // For command-line options, if you specify a flag option, you're saying it is true
        // but we allow overriding to false when passing "0" following the flag
        if (it->second.empty() || (it->second == string("0"))) {
            val = false;
        } else {
            val = true;
        }
        return true;
    }

    // Retrieve a signed integer option value
    bool GetOption(const string &label, int64_t &val)
    {
        map<string, string>::iterator it = options.find(label);
        if (it == options.end()) {
            return false;
        }
        string opt, msg;
        return Extractor.ExtractInt64(it->second, opt, val, msg);
    }

    // Retrieve an unsigned option value, which could be a size
    // parameter allowSizeSuffix tells us Whether we allow k/m/g suffix
    bool GetOption(const string &label, uint64_t &val, bool allowSizeSuffix = true)
    {
        map<string, string>::iterator it = options.find(label);
        if (it == options.end()) {
            return false;
        }
        string opt, msg;
        if (allowSizeSuffix) {
            return Extractor.ExtractSize(it->second, opt, val, msg);
        }
        return Extractor.ExtractUint64(it->second, opt, val, msg);
    }

    // Retrieve a double precision, signed floating point value
    bool GetOption(const string &label, double &val)
    {
        map<string, string>::iterator it = options.find(label);
        if (it == options.end()) {
            return false;
        }
        string opt, msg;
        return Extractor.ExtractFloat(it->second, opt, val, msg);
    }

    // Allow an option to be used.
    void SetOption(const string &label, const string &opt, OptionType type)
    {
        options[label] = opt;
        types[label] = type;
    }

    // Reset an individual option - it won't be a legal option once ResetOption() is called
    void ResetOption(const string &label)
    {
        options.erase(label);
    }

    // Clear all options - none will be allowed once Reset() is called
    void Reset()
    {
        options.clear();
        types.clear();
        extra.clear();
    }

    // copy a set of options
    // (i.e. copy global MASH options to be potentially overidden per-shell-command
    void Clone(const OptionsSet& other)
    {
        options = other.options;
        types = other.types;
        extra = other.extra;
    }

    // Print all available options - useful for deubgging
    void PrintOptions()
    {
        map<string, string>::iterator optit = options.begin();
        map<string, OptionType>::iterator typeit = types.begin();
        for (; optit != options.end(); ++optit, ++typeit) {
            fprintf(OutputFilePtr, "Option [%s] = %s, type: %s\n",
                optit->first.c_str(), optit->second.c_str(), getTypeName(typeit->second).c_str());
        }
        for (uint32_t i = 0;i < extra.size(); ++i) {
            fprintf(OutputFilePtr, "Extra Arg: %s\n", extra[i].c_str());
        }
    }

};

// Unique Option label list
namespace MdbmOptions {
// List of all option label values
static     string InitialSize("initial-size");  // a.k.a. presize in MDBM terminology
static     string LimitSize("limit-size");
static     string HashFunc("hash-func");
static     string LockMode("lock-mode");
static     string HelpFlag("help");
static     string SpillSize("spill-size");
static     string LargeObj("large-object");
static     string PageSize("page-size");
static     string MainDBSize("main-db-size");
static     string WindowSize("window-size");
static     string CacheMode("cache-mode");
static     string DataFormat("data-format");
static     string RecAlign("record-alignment");
static     string FilePermissions("file-mode");
static     string TruncateDB("truncate-db");
static     string OutputFile("output-file");
static     string EraseZeroLen("delete-empty");
static     string InputFile("input-file");
static     string StoreMode("store-mode");
static     string NoDbHeader("no-header");
static     string IntegrityCheckLevel("check-level");
static     string PageNumber("page-number");
static     string ShowMdbmVersion("mdbm-version");
static     string VerbosityLevel("verbose");
static     string VerifyVersion("verify-version");
static     string PrintKeys("show-keys");
static     string PrintValues("show-values");
static     string CompareType("compare-mode");
static     string ReadOnlyAccess("read-only");
static     string StatType("stat-type");
static     string ExtraData("extra");
static     string Force("force");
}

using namespace MdbmOptions;

/**
 * Note: The OptionParser class is separate from OptionSet so that we can separately parse
 *   in-shell commands from implicit commands where, in the future, we might implement 'mdbm_dump'
 *   as a sym-linked alias to 'mash'.  Another reason we wrote a brand new option-parsing and
 *   handling class is that getopt_long does not allow you to specify and verify against
 *   option-type information such as whether an option is expected to be an unsigned/signed
 *   integer, float, string, size, or flag, which means that type-verification code has to be
 *   repeated all over the place.
 * Having this class will also allow us to eventually implement value completion, with
 *   methods Set/CheckAllowedValues being used to define the range of allowed values.
 */
class OptionParser
{
public:
    map<string, OptionDescriptor> knownOptions;  // indexed by "label"
    OptionsSet optionsData;

protected:
    // Maintain a secondary map for quick lookups by the single-letter option name "opt"
    map<string, OptionDescriptor *> optionsByName;

    map<string, uint64_t> maxValues;   // If set, Maximum allowed value of an unsigned int

   // If set, a set of strings that are allowed values
    map<string, set<string> > allowedValues;

public:

    OptionParser()
    {
        // All commands support the "-h" or the "--help" option
        RegisterOption(string("h"), OPTION_TYPE_FLAG, HelpFlag, string("This message"));
    }

    virtual ~OptionParser() {}

    const char *ItoS(unsigned long long val)
    {
        static char buf[64];
        snprintf(buf, sizeof(buf), "%llu", val);
        return buf;
    }

    // add an option to the list of known options
    void RegisterOption(const string &opt, OptionType type, const string &label,
                        const string &longDescription)
    {
        OptionDescriptor newOption;
        newOption.option = opt;
        newOption.type = type;
        newOption.label = label;
        newOption.longDescription = longDescription;

        knownOptions[label] = newOption;   // Update maps indexed by long and short option name
        optionsByName[opt] = &(knownOptions[label]);
    }

    // Remove an unnecessary option from the list of supported options
    void DeleteOption(const string &label)
    {
        map<string, OptionDescriptor>::iterator it = knownOptions.find(label);
        if (it == knownOptions.end()) {
            return;
        }
        optionsByName.erase(it->second.option);
        knownOptions.erase(label);
        maxValues.erase(label);
        allowedValues.erase(label);
    }

    // Get a string that displays the list of supported options and their description
    string GetOptionList()
    {
        string result;
        map<string, OptionDescriptor>::iterator it = knownOptions.begin();
        for (;it != knownOptions.end(); ++it) {
            OptionDescriptor cur = it->second;
            // Label is long option name.  For flags we print shortOpt|longOpt
            string labl = string("|") + cur.label + string("\t");
            //  <ArgType> which is also the long option appears for non-flags
            if (cur.type != OPTION_TYPE_FLAG) {
                string ending(" value");
                if (cur.type == OPTION_TYPE_SIZE) {   // Size doesn't need a "value"
                    ending.clear();
                }
                labl += string("<") + getTypeName(cur.type) + ending + string(">\t");
            }
            result += string("\t-") + cur.option + labl + cur.longDescription + string("\n");
        }
        return result;
    }

    // Set the Maximum allowed value for a given integer value option
    void SetMaxAllowed(const string &label, uint64_t highestValue)
    {
        maxValues[label] = highestValue;
    }

    // Check against the maximum allowed value for a given option, if specified by
    // SetMaxAllowed()
    bool CheckMaxAllowed(const string &label, uint64_t value)
    {
        map<string, uint64_t>::iterator maxValIt = maxValues.find(label);
        if (maxValIt == maxValues.end()) {
            return true;   // No maximum value restriction, so value is good
        }
        return (value <= maxValIt->second);
    }

    // Optionally specify the set of allowed values a string option can have
    void SetAllowedValues(const string &label, const set<string> &values)
    {
        allowedValues[label] = values;
    }

    /**
     * Check against the range of allowed string values an option can have, if specified
     * by SetAllowedValues().  Checking is case insensitive.
     *  \param[in] label - The label (long-option) being checked
     *  \param[in] value - The value being checked vs. allowed range set by SetAllowedValues()
     *  \return true if no range specified or it is one of the allowed values, false otherwise.
     */
    bool CheckAllowedValues(const string &label, const string &value)
    {
        // Convert option value (option name is case sensitive, but the value isn't)
        // to lowercase before checking against the allowed string values.
        string lowerc;
        for (uint32_t i = 0; i < value.size(); ++i) {
            lowerc += tolower(value[i]);
        }
        map<string, set<string> >::iterator valRangeIt = allowedValues.find(label);
        if (valRangeIt == allowedValues.end()) {
            return true;   // No values list restriction, so value OK
        }
        set<string>::iterator foundIt = valRangeIt->second.find(lowerc);
        return foundIt != valRangeIt->second.end();
    }

    /**
     * Take a vector of strings, then parse it an populate command line options and "extra",
     * which stores the non-option values (e.g. MDBM file name in mfetch...)
     *  \param[in] args       - the list of unprocessed values passed to a Mash command
     *  \param[in] startIndex - parsing starts at args[startIndex]
     *  \param[out] errorMsg  - If an error occurs while parsing, ParseOptions() will populate
     *                          the errorMsg string so that a meaningful error msg can be shown.
     *  \return - true if options were successfully parse, false if not successful
     */
    bool ParseOptions(const vector<string> &args, int startIndex, string &errorMsg)
    {
        for (unsigned i = startIndex; i < args.size(); ++i) {
            bool longOption;
            if (IsOptionGetType(args[i], longOption)) {  // Is it an option and is it a long one
                string opt = GetOptionName(args[i]);
                if (opt.size() == 0) {
                    errorMsg = string("Invalid option format: ") + args[i];
                    return false;
                }
                OptionDescriptor *optPtr;      // Pointer to option found
                if (longOption) {   // long options, find in knownOptions
                    map<string, OptionDescriptor>::iterator curopt = knownOptions.find(opt);
                    if (curopt == knownOptions.end()) {
                        errorMsg = string("Invalid option: ") + args[i];
                        return false;
                    }
                    optPtr = &(curopt->second);
                } else {  // single-letter (short) options, find in optionsByName
                    map<string, OptionDescriptor*>::iterator curopt = optionsByName.find(opt);
                    if (curopt == optionsByName.end()) {
                        errorMsg = string("Invalid option: ") + args[i];
                        return false;
                    }
                    optPtr = curopt->second;
                }
                // Make sure you have an additional arg, except for on/off flag
                if ((i >= args.size() - 1) && (optPtr->type != OPTION_TYPE_FLAG)) {
                    char buffer[120];
                    snprintf(buffer, sizeof(buffer), "Missing %s arg following option -%s",
                             optPtr->label.c_str(), opt.c_str());
                    errorMsg = buffer;
                    return false;
                }
                string value;
                uint64_t unsignedVal;
                int64_t signedVal;
                double floatVal;
                switch (optPtr->type) {
                    case OPTION_TYPE_STRING:
                        value = args[++i];    // advance i
                        if (!CheckAllowedValues(optPtr->label, value)) {
                            errorMsg = string("Invalid ") + optPtr->label +
                                       string(" following option -") + opt;
                            return false;
                        }
                        break;
                    case OPTION_TYPE_FLAG:
                        // If you specify a flag option, you're saying it is true
                        // but we allow overriding to false when passing "0" following the flag
                        // Allow 0/1 following binary flags, and advance i
                        if (((i+1) < args.size()) &&
                            ((args[i+1] == string("0")) || (args[i+1] == string("1")))) {
                            value = args[++i];    // Advance i if 0/1 specified
                        } else {
                            value = string("1");  // Store true(1) if not specified
                        }
                        break;
                    case OPTION_TYPE_UNSIGNED:
                        if (!Extractor.ExtractUint64(args[i+1], opt, unsignedVal, errorMsg)) {
                            return false;
                        } else {   // Check against maximum allowed, if any
                            if (!CheckMaxAllowed(optPtr->label, unsignedVal)) {
                                errorMsg = string("Value ") +
                                           ItoS(unsignedVal) + string(" too big");
                                return false;
                            }
                            value = args[++i];    // advance i
                        }
                        break;
                    case OPTION_TYPE_SIGNED:
                        if (!Extractor.ExtractInt64(args[i+1], opt, signedVal, errorMsg)) {
                            return false;
                        } else {   // Check against maximum allowed, if any
                            if (!CheckMaxAllowed(optPtr->label, signedVal)) {
                                errorMsg = string("Value ") +
                                           ItoS(signedVal) + string(" too big");
                                return false;
                            }
                            value = args[++i];    // advance i
                        }
                        break;
                    case OPTION_TYPE_FLOAT:
                        if (!Extractor.ExtractFloat(args[i+1], opt, floatVal, errorMsg)) {
                            return false;
                        } else {
                            value = args[++i];    // advance i
                        }
                        break;
                    case OPTION_TYPE_SIZE:
                        if (!Extractor.ExtractSize(args[i+1], opt, unsignedVal, errorMsg)) {
                            return false;
                        } else {   // Check against maximum allowed, if any
                            if (!CheckMaxAllowed(optPtr->label, unsignedVal)) {
                                errorMsg = string("Value ") +
                                           ItoS(unsignedVal) + string(" too big");
                                return false;
                            }
                            value = args[++i];    // advance i
                        }
                        break;
                    default: // NOTE: stack error check thinks this block is unreachable
                        char buffer[80];
                        snprintf(buffer, sizeof(buffer), "Invalid option type %d", optPtr->type);
                        errorMsg = buffer;
                        return false;
                }
                optionsData.SetOption(optPtr->label, value, optPtr->type);
            } else {  // Not an option, then add to "extra", which is the vector of non-options
                optionsData.extra.push_back(args[i]);
            }
        }
        return true;
    }

    // Specify common Locking mode options of -L/--lock-mode and their allowed range
    virtual void AddLockModeOption()
    {
        // option to set lock mode: descriptive name is "LockMode"
        RegisterOption(string("L"), OPTION_TYPE_STRING, LockMode,
                       string("Locking mode:\n"
"                   exclusive - Exclusive locking (default)\n"
"                   partition - Partition locking\n"
"                   shared    - Shared locking\n"
"                   any       - use whatever locks exist\n"
"                   none      - no locking"));
        set<string> lockModeValues;
        lockModeValues.insert("exclusive");
        lockModeValues.insert("partition");
        lockModeValues.insert("shared");
        lockModeValues.insert("any");
        lockModeValues.insert("none");
        SetAllowedValues(LockMode, lockModeValues);
    }

    // Commonly used option to output using cdb or dbdump formats
    virtual void AddDataFormatOption()
    {
        RegisterOption(string("F"), OPTION_TYPE_STRING, DataFormat,
                       string("cdb = use cdb format (default is dbdump format)"));
        set<string> formats;
        formats.insert("cdb");
        formats.insert("dbdump");
        SetAllowedValues(DataFormat, formats);
    }

    // Implemented in subclass
    virtual bool GetCreateOptions(int &flags, int &cacheModeParam, uint64_t &preSize,
                     uint64_t &pageSize, uint64_t &mainDBSize, uint64_t &spillSize,
                     int lockMode = 0)   // Exclusive locking by default
    {
        return false;   // Returns false because it should not be called
    }

    // Generic method to get lock flags.  MDBM_ANY_LOCKS is default for the case when
    // lock mode is not specified.
    virtual int GetLockFlags(int defaultFlag = MDBM_ANY_LOCKS)
    {
        int lockFlags = defaultFlag;
        string lockMode;
        if (optionsData.GetOption(LockMode, lockMode)) {
            mdbm_util_lockstr_to_flags(lockMode.c_str(), &lockFlags);
        }
        return lockFlags;
    }
};

// main() uses the OptionParser class's capabilities to parse its args: -h, -i, or -o
class MainOptionParser : public OptionParser
{
public:
    MainOptionParser()
    {
        RegisterOption(string("h"), OPTION_TYPE_FLAG, HelpFlag, string("This message"));
        RegisterOption(string("i"), OPTION_TYPE_STRING, InputFile,
                       string("Accept input from named file instead of standard input"));
        RegisterOption(string("o"), OPTION_TYPE_STRING, OutputFile,
                       string("Write to <output-file> instead of standard output/error"));
    }
};

void
ProcessInputOption(const string &inputFile, bool outputToFile)
{
    FILE *infp = NULL;
    if (inputFile.size()) {
        string inpath(Utils.FullPath(inputFile));
        if ((infp = fopen(inpath.c_str(), "r")) == NULL) {
            fprintf(stderr, "Cannot open %s: %s\n", inpath.c_str(), strerror(errno));
            exit(2);
        }
        // Get one line at a time from the input file and run each command
        const int SCRIPT_MAX_LINE = 4096;
        char buf[SCRIPT_MAX_LINE];
        while ((DoneProcessing == false) && fgets(buf, SCRIPT_MAX_LINE, infp)) {
            string line(buf);
            Utils.Trim(line);
            if (line.size()) {
                if (outputToFile) {
                    fprintf(OutputFilePtr, ">>  %s\n", line.c_str());
                }
                execute_line(line);
            }
        }
        fclose(infp);
        MdbmData.CloseOpenMdbms();
    }
}


int main(int argc, char** argv)
{
    char pwdbuf[MAXPATHLEN];
    if (getcwd(pwdbuf, MAXPATHLEN) > 0) {
        PwdStr = pwdbuf;
    }

    if (argc > 1) {
        vector<string> args;
        for (int i=0; i < argc; ++i) {
            args.push_back(argv[i]);
        }
        MainOptionParser parser;
        string errorMsg;
        if (!parser.ParseOptions(args, 1, errorMsg)) {
            fprintf(stderr, "Error: %s, exiting\n", errorMsg.c_str());
            exit(1);
        }
        bool help = false;
        parser.optionsData.GetOption(HelpFlag, help);
        if (help) {
            fprintf(stdout, "mash [Options Below]\n%s\n", parser.GetOptionList().c_str() );
            exit(0);
        }

        bool outputToFile = false;   // Set a flag telling us to output commands
        FILE *outfp = NULL;
        string outputFile;   // Look for an output file name and open it
        if (parser.optionsData.GetOption(OutputFile, outputFile)) {
            string outpath(Utils.FullPath(outputFile));
            if ((outfp = fopen(outpath.c_str(), "w")) == NULL) {
                fprintf(ErrorFilePtr, "Cannot open %s: %s\n", outpath.c_str(), strerror(errno));
                exit (1);
            }
            ErrorFilePtr = OutputFilePtr = outfp;
            outputToFile = true;
        }
        string inputFile;   // Input file name
        if (parser.optionsData.GetOption(InputFile, inputFile)) {
            ProcessInputOption(inputFile, outputToFile);
        }
        if (outfp) {
            fclose(outfp);
        }
        return 0;
   }

    initialize_readline(); // Bind our completer.

    // Loop reading and executing lines until the user quits.
    while (DoneProcessing == false) {
        //  //re-enable auto-complete, in case the completion function had to disable it
        //  rl_bind_key('\t',rl_complete);

        char* tmp = readline(">>  ");
        if (!tmp) {
            break;
        }
        string line = tmp;
        free(tmp);
        // Remove leading and trailing whitespace.
        // if there is anything left, add it to the history list and execute it.
        Utils.Trim(line);
        if (line.size()) {
            add_history(line.c_str());
            execute_line(line);
        }
    }

    // Need to close all open MDBMs here
    MdbmData.CloseOpenMdbms();

    return(0);
}

// Generic option parser specifying all options useful for creating new MDBMs
class MdbmOptionParser : public OptionParser
{
public:
    MdbmOptionParser()
    {
        // Set the cache mode
        RegisterOption(string("C"), OPTION_TYPE_STRING, CacheMode,
                       string("Set cache mode: (LFU, LRU, or GDSF)"));
        set<string> cacheModeValues;
        cacheModeValues.insert("lfu");
        cacheModeValues.insert("lru");
        cacheModeValues.insert("gdsf");
        SetAllowedValues(CacheMode, cacheModeValues);
        // option to set initial MDBM size
        RegisterOption(string("d"), OPTION_TYPE_SIZE, InitialSize,
                       string("Initial size of MDBM, followed by optional k/m/g"));
        // option to set maximum MDBM size: descriptive name is "LimitSize"
        RegisterOption(string("D"), OPTION_TYPE_SIZE, LimitSize,
                       string("Maximum size of MDBM, followed by optional k/m/g"));

        // option to set hash function: descriptive name is "HashFunc" constant
        RegisterOption(string("H"), OPTION_TYPE_UNSIGNED, HashFunc,
                       string("Hash function (default 5/FNV32)\n"
"                   0 CRC-32\n"
"                   1 EJB\n"
"                   2 PHONG\n"
"                   3 OZ\n"
"                   4 TOREK\n"
"                   5 FNV32\n"
"                   6 STL\n"
"                   7 MD5\n"
"                   8 SHA-1\n"
"                   9 Jenkins\n"
"                  10 Hsieh SuperFast"));
        SetMaxAllowed(HashFunc, 10);

        AddLockModeOption();

        RegisterOption(string("l"), OPTION_TYPE_SIZE, SpillSize,
                       string("Size of value above which the value goes into large object store"));

        RegisterOption(string("O"), OPTION_TYPE_FLAG, LargeObj,
                       string("Enable large object support"));
        RegisterOption(string("p"), OPTION_TYPE_SIZE, PageSize,
                       string("Page size (default 4096)"));
        SetMaxAllowed(PageSize, MDBM_MAXPAGE);
        RegisterOption(string("s"), OPTION_TYPE_SIZE, MainDBSize,
                       string("Size of main DB (remainder is large object/oversized"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
    }

    // Get all options values, if available, for creating new MDBMs.
    // Set options values to default if option has not been specified.
    // The "flags" param is modified to reflect large objects, windowed mode, etc.
    // Return false if errors found, true if OK
    virtual bool GetCreateOptions(int &flags, int &cacheModeParam, uint64_t &preSize,
                     uint64_t &pageSize, uint64_t &mainDBSize, uint64_t &spillSize,
                     int lockMode = 0)   // Exclusive locking by default for mcreate
    {
        // Cache mode option
        string cacheMode;
        cacheModeParam = 0;
        // Already validated and getting strings won't fail
        if (optionsData.GetOption(CacheMode, cacheMode)) {
            if (strcasecmp(cacheMode.c_str(), "lfu") == 0) {
                cacheModeParam = MDBM_CACHEMODE_LFU;
            } else if (strcasecmp(cacheMode.c_str(), "lru") == 0) {
                cacheModeParam = MDBM_CACHEMODE_LRU;
            } else if (strcasecmp(cacheMode.c_str(), "gdsf") == 0) {
                cacheModeParam = MDBM_CACHEMODE_GDSF;
            }
        }

        preSize = 0;   // Presize option
        optionsData.GetOption(InitialSize, preSize);

        if (preSize && (preSize > (uint64_t) INT_MAX))  {
            preSize /= (1024*1024);
            flags |= MDBM_DBSIZE_MB;
        }

        uint64_t limitSize = 0;  // Limit size option
        optionsData.GetOption(LimitSize, limitSize);

        int lockFlags = GetLockFlags(lockMode);
        flags |= lockFlags;

        bool largeObj = false;   // Flag indicating whether Large objects are supported
        optionsData.GetOption(LargeObj, largeObj);
        if (largeObj) {
            flags |= MDBM_LARGE_OBJECTS;
        }

        optionsData.GetOption(PageSize, pageSize);  // Get the page size
        if (pageSize == 0) {           // In case someone explicitly set page size to zero
            pageSize = MDBM_PAGESIZ;   // Default to page size of 4K
        }

        mainDBSize = 0;  // Get the main DB size
        optionsData.GetOption(MainDBSize, mainDBSize);
        if (mainDBSize) {
            int pages = mainDBSize / pageSize;
            if ((mainDBSize % pageSize) || (pages & (pages -1))) {
                mainDBSize = 0;
                fprintf(ErrorFilePtr, "main DB size must have power-of-2 database pages, ignoring\n");
            }
            if (mainDBSize > limitSize) {
                mainDBSize = 0;
                fprintf(ErrorFilePtr, "main DB size must be smaller than the limit size, ignoring\n");
            }
        }

        spillSize = 0;  // Get the spill size (size above which values go into large object store
        if (optionsData.GetOption(SpillSize, spillSize) && !largeObj) {
            spillSize = 0;
            fprintf(ErrorFilePtr, "Missing large object flag, ignoring spill size\n");
        }
        // If you want large objects, you need to specify a spill size, but Mash will set it
        // to the 3/4 page size default for you
        if (largeObj && !spillSize) {
            spillSize = pageSize * 3 / 4;
        }

        uint64_t windowSize = 0;  // Get the window size
        optionsData.GetOption(WindowSize, windowSize);
        if (windowSize) {
            flags |= MDBM_OPEN_WINDOWED;
        }
        return true;
    }
};


/// MdbmCommand class defines the common code belonging to MDBM-specific Mash commands
/// (all the non Unix-command, such as ls, pwd,cd..., specific functionality)

class MdbmCommand: public MashCommand
{
public:

    MdbmCommand(const char* cmdName, const char* cmdDesc) : MashCommand(cmdName, cmdDesc) { }

    virtual ~MdbmCommand()
    {
        if (parser) {
            delete parser;
        }
    }

    virtual string GetFullPath(const string &path)
    {
        if (path == CurrentWorkingSymbol) {
            return MdbmData.GetCurrentMdbmFile();
        }
        return MashCommand::GetFullPath(path);
    }

    string GetOptionList()
    {
        return parser->GetOptionList() + string("\n");
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] YourFile.mdbm\n");
    }

    virtual bool CheckHelp()
    {
        bool help = false;
        parser->optionsData.GetOption(HelpFlag, help);
        if (help) {
            fprintf(ErrorFilePtr, "\t%s\nUsage:\n%s%s",  desc.c_str(),
                    GetCommandFormat().c_str(), GetOptionList().c_str());
        }
        return help;
    }

    // Convert a single arg into a vector of args and Run(), because for
    // all MDBM-commands, there is no difference between Run(string) and Run(vector<string>)
    virtual int Run(const string &arg)
    {
        vector <string> args;
        args.push_back(arg);
        return Run(args);
    }

    // Subclasses need to overload this class to perform their MDBM command
    virtual int Run(const vector<string> &args)
    {
        return -1;
    }

    // Use ParseVerify to start processing and handle the "-h" arg for any Mash command
    // such as mexport/mimport/mcheck/mcompare... that can only accept only a
    // pre-determined number of MDBM parameters.
    // paramCount mostly defaults to 1, except for commands such as 
    // mcompare and mcopy, which need 2.
    // If isMinimum is true, paramCount is the minimum number of parameters
    virtual int ParseVerify(const vector<string> &args, string &usageMsg, int &stopNow,
                            unsigned int paramCount = 1, bool isMinimum = false)
    {
        parser->optionsData.Reset();
        string errorMsg;

        usageMsg = string("usage:\n");
        usageMsg += GetCommandFormat();
        usageMsg += GetOptionList();   // Construct the "usage" message

        if (!parser->ParseOptions(args, 1, errorMsg)) {
            fprintf(ErrorFilePtr, "%s failed: %s.  Correct %s",
                    cmdName.c_str(), errorMsg.c_str(), usageMsg.c_str());
            return -1;
        }

        if (CheckHelp()) {   // Help means no error occurred, but we should signal
            stopNow = 1;     // that the Mash command should be stopping here.
            return 0;
        }

        unsigned int siz = parser->optionsData.extra.size();
        string errType;
        if (siz == 0) {
            errType = "Missing filename";
        } else if (siz < paramCount) {
            errType = "Too few parameters";
        } else if (!isMinimum && (siz > paramCount)) {
            errType = "Too many parameters";
        }
        if (errType.size()) {
            fprintf(ErrorFilePtr, "%s, Correct %s", errType.c_str(), usageMsg.c_str());
            return -1;
        }
        return 0;
    }

    // Retrieve the -w option, if there, and set the window size if necessary
    virtual void SetWindowSize(MDBM *db)
    {
        uint64_t windowSize = 0;  // Get the window size
        parser->optionsData.GetOption(WindowSize, windowSize);

        if (windowSize && mdbm_set_window_size(db, (size_t) windowSize)) {
            fprintf(ErrorFilePtr, "%s: Cannot set window size\n", mdbm_get_filename(db));
        }
    }

    /**
     * Select the locking mode, then mdbm_open() using param mdbmFile as the MDBM file name.
     * fullPath is set to the fully qualified path of mdbmFile.
     * Then, sets windowed mode/size, if selected via command line args.
     *  \param[in]  extraFlags - extra flags that do no specify RW/RDONLY, lock-mode, window-mode
     *  \param[in]  mdbmFile   - name of MDBM file to access.  May not be absolute path.
     *  \param[out] fullPath   - fully qualified path of MDBM file to access
     *  \param[in]  readOnly   - open in read-only mode if true, RW otherwise, except when
     *                           windowed mode is specified, which overrides readOnly=true
     */
    virtual MDBM *OpenExisting(int extraFlags, const string &mdbmFile, string &fullPath,
                                         bool readOnly = true)
    {
        fullPath = GetFullPath(mdbmFile);
        int flags = extraFlags;

        uint64_t windowSize = 0;  // Get the window size
        parser->optionsData.GetOption(WindowSize, windowSize);

        if (windowSize) {   // Windowed mode requires R/W access
            flags |= MDBM_O_RDWR | MDBM_OPEN_WINDOWED;
        } else if (readOnly) {
            flags |= MDBM_O_RDONLY;
        } else {
            flags |= MDBM_O_RDWR;
        }

        int lockFlags = parser->GetLockFlags();  // Default: LOCKS_ANY
        MDBM *db = MdbmData.FindHandle(fullPath);
        if (db == NULL) {
            db = mdbm_open(fullPath.c_str(), flags | lockFlags, 0, 0, 0);
        }
        if (db == NULL) {
            fprintf(ErrorFilePtr, "Unable to open MDBM %s, lockFlags(%d): %s\n",
                    fullPath.c_str(), lockFlags, strerror(errno));
            return NULL;
        }
        MdbmData.StoreHandle(db);
        MdbmData.SetCurrentMdbmFile(fullPath);

        if (windowSize && mdbm_set_window_size(db, (size_t) windowSize)) {
            fprintf(ErrorFilePtr, "%s: Cannot set window size after opening\n", fullPath.c_str());
        }

        return db;
    }

    // Open or create an MDBM based on a standard set of optional command
    // line args (e.g. page size, limit size, large obj...) defined and processed
    // by the MdbmParser class.  If MDBM exists, will attempt to change "changeable" MDBM
    // params like limit size, but not others like page size.
    // MDBM must be opened successfully, but for other settings OpenDBWithParams()
    // will print error msgs but continue processing.
    virtual MDBM *OpenDBWithParams(const char *filename, int flags, int mode,
                                     int cacheModeParam, uint64_t preSize,
                                     uint64_t pageSize, uint64_t mainDBSize, uint64_t spillSize)
    {
        MDBM *db = NULL;
        if ((db = mdbm_open(filename, flags, mode, (int) pageSize, (int) preSize)) == NULL) {
            fprintf(ErrorFilePtr, "Cannot open %s (%s)\n", filename, strerror(errno));
            return NULL;
        }

        uint64_t hashFunc = MDBM_DEFAULT_HASH;
        if (parser->optionsData.GetOption(HashFunc, hashFunc) &&
            (mdbm_sethash(db, (int) hashFunc) != 1)) {
            fprintf(ErrorFilePtr, "%s: Cannot set hash to %d\n", filename, (int) hashFunc);
        }

        uint64_t limitSize = 0;  // Limit size option
        parser->optionsData.GetOption(LimitSize, limitSize);
        if (limitSize) {
            int pages = limitSize / pageSize;
            if (mdbm_limit_size_v3(db, pages, NULL, NULL)) {
                fprintf(ErrorFilePtr, "%s: Cannot set limit size\n", filename);
            }
        }

        SetWindowSize(db);
        if (mainDBSize) {
            int pages = mainDBSize / pageSize;
            if (mdbm_limit_dir_size(db, pages)) {
                fprintf(ErrorFilePtr, "%s: Cannot set directory size\n", filename);
            }
            if (mdbm_pre_split(db, pages)) {
                fprintf(ErrorFilePtr, "%s: Cannot split DB\n", filename);
            }
        }
        if (cacheModeParam && mdbm_set_cachemode(db, cacheModeParam)) {
            fprintf(ErrorFilePtr, "%s: Cannot set cachemode to %d\n", filename, cacheModeParam);
        }
        if (spillSize && mdbm_setspillsize(db, (int) spillSize)) {
            fprintf(ErrorFilePtr, "%s: Cannot set spill size\n", filename);
        }
        MdbmData.SetCurrentMdbmFile(filename);
        return db;
    }

    // If an MDBM doesn't exist already, create an MDBM based on a standard set of optional
    // command line args (e.g. page size, limit size, large obj...) defined and processed
    // by the MdbmParser::GetCreateOptions().  If MDBM exists, will attempt to change
    // "changeable" MDBM params like limit size, but not others like page size.
    // This is a convenience wrapper around GetCreateOptions() and OpenDBWithParams() combo.
    virtual MDBM *CreateMdbm(int flags, int mode, uint64_t &pageSize)
    {
        flags |= MDBM_O_RDWR | MDBM_O_CREAT;
        int cacheModeParam;
        uint64_t preSize, mainDBSize, spillSize;

        if (!parser->GetCreateOptions(flags, cacheModeParam, preSize,
                                      pageSize, mainDBSize, spillSize)) {
            return NULL;
        }

        string fullPath(GetFullPath(parser->optionsData.extra[0]));
        return OpenDBWithParams(fullPath.c_str(), flags, 0666, cacheModeParam,
                                  preSize, pageSize, mainDBSize, spillSize);
    }

    // ConvertDatum will convert any unprintable character or backslash to its 2 hex-digit code.
    // The hex code will have the format of: \x##
    // where each # is a hex digit (0-F), so:
    // The NULL character (\0) is \x00, newline (\n) is \x0A, backslash is \x5c
    // Tab (\t) is \x09, and space is a printable character.
    bool ConvertDatumToHex(datum &dt)
    {
        int len = 0;
        for(int i = 0; i < dt.dsize; ++i) {
            if (isprint(dt.dptr[i]) && (dt.dptr[i] != '\\')) {
                ++len;
            } else {
                len += 4;   // Need 4 chars for \x##
            }
        }
        datum newDatum;
        newDatum.dsize = len;
        if ((newDatum.dptr = (char *) malloc(len)) == NULL) {
            return false;
        }
        for(int i = 0, j = 0; i < dt.dsize && j < len; ++i) {
            if (isprint(dt.dptr[i]) && (dt.dptr[i] != '\\')) {
                newDatum.dptr[j++] = dt.dptr[i];
            } else {
                // 4 chars of \x##
                newDatum.dptr[j++] = '\\';
                newDatum.dptr[j++] = 'x';
                char buf[4];
                snprintf(buf, 4, "%02x", dt.dptr[i]);
                newDatum.dptr[j++] = buf[0];
                newDatum.dptr[j++] = buf[1];
            }
        }
        dt = newDatum;
        return true;
    }

    // Convenience method to convert key+value to hex format
    // while returning true if converted to hex, false otherwise
    bool ConvertToHexFormat(datum &key, datum &val, bool chars2hex)
    {
        if (!chars2hex) {
            return false;
        }

        bool ret = ConvertDatumToHex(key);
        return ConvertDatumToHex(val) && ret;
    }

    // Convert data that may contain "hex codes" in param hexCodeString, into a string.
    // So anything starting with \x## where # is a hex digit gets converted
    // to its single-character binary value. The return value is  the converted string.
    string GetStringFromHex(const string &hexCodeString)
    {
        string val;
        size_t i = 0, siz = hexCodeString.size();
        while (i < siz) {
            if (hexCodeString[i] != '\\') {
                val.append(1, hexCodeString[i]);
                ++i;
            } else {
                if (((i + 3) < siz) && (hexCodeString[i+1] == 'x')) {
                    val.append(1,
                               mdbm_internal_hex_to_byte(hexCodeString[i+2], hexCodeString[i+3]));
                    i += 4;
                } else {
                    break;
                }
            }
        }
        return val;
    }

protected:
    OptionParser *parser;
};


// MdbmSimpleCommand class allows you to run very simple MDBM-specific Mash commands by
// subclassing and overloading virtual method workFunc() that the subclass implements.

class MdbmSimpleCommand: public MdbmCommand
{
public:
    MdbmSimpleCommand(const char* cmdName, const char* cmdDesc) : MdbmCommand(cmdName, cmdDesc)
    {
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] YourFile1.mdbm YourFile2.mdbm ...\n");
    }

    // Simple "Run(vector<string>args)" method that supports only the -h option.
    //       Will iterate through its args and apply workFunc on each one.
    //   Arguments:
    //     - args      contain the command-line arguments, which are generally the filename(s)
    //     - workFunc  actually does the work that is specific to the particular command
    //     - doingWhat tells SimpleRun() what is being done so it can generate a meaningful
    //                 error message.
    virtual int SimpleRun(const vector<string> &args, const string &doingWhat)
    {
        parser->optionsData.Reset();
        string errorMsg;

        string usageMsg("usage:\n");
        usageMsg += GetCommandFormat();
        usageMsg += GetOptionList();

        if (!parser->ParseOptions(args, 1, errorMsg)) {
            fprintf(ErrorFilePtr, "%s failed: %s.  Correct %s",
                    cmdName.c_str(), errorMsg.c_str(), usageMsg.c_str());
            return -1;
        }

        if (CheckHelp()) {
            return 0;
        }

        int fileCount;
        // At least one MDBM file name is required
        if ((fileCount = parser->optionsData.extra.size()) < 1) {
            fprintf(ErrorFilePtr, "Missing filename, Correct %s", usageMsg.c_str());
            return -1;
        }

        // iterate through all non command-line option args, assuming they are MDBM file names.
        for (int i = 0; i < fileCount; ++i) {
            string fullPath(GetFullPath(parser->optionsData.extra[i]));
            const char *fname = fullPath.c_str();
            if (workFunc(fname)) {
                fprintf(ErrorFilePtr, "Failed when %s %s: %s\n",
                        doingWhat.c_str(), fname, strerror(errno));
                return (-1);
            }
            MdbmData.SetCurrentMdbmFile(fullPath);
        }

        return 0;
    }

protected:
    virtual int (workFunc)(const char *) = 0;

};

///  mcreate (mdbm_create with standardized options)
// MdbmCreateParser subclasses MdbmOptionParser, which supports most file-creating
// command-line options
class MdbmCreateParser : public MdbmOptionParser
{
public:
    MdbmCreateParser()
    {
        RegisterOption(string("a"), OPTION_TYPE_UNSIGNED, RecAlign,
                       string("Set records to be aligned to 1, 2, 4, or 8 bytes"));
        SetMaxAllowed(RecAlign, 8);

        RegisterOption(string("m"), OPTION_TYPE_STRING, FilePermissions,
                       string("Set file permissions (default: 0666)"));
        RegisterOption(string("Z"), OPTION_TYPE_FLAG, TruncateDB,
                       string("Truncate DB if already exists"));
    }
};

// mcreate command handling class
class MdbmCreateCommand : public MdbmCommand
{
public:
    MdbmCreateCommand() : MdbmCommand("mcreate", "Create a new MDBM data file")
    {
        parser = new MdbmCreateParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] YourFile1.mdbm YourFile2.mdbm ...\n");
    }

    virtual int Run(const vector<string> &args)
    {
        parser->optionsData.Reset();
        string errorMsg;

        string usageMsg("Usage:\n");
        usageMsg += GetCommandFormat();
        usageMsg += GetOptionList();

        if (!parser->ParseOptions(args, 1, errorMsg)) {
            fprintf(ErrorFilePtr, "%s failed: %s.  Correct %s",
                    cmdName.c_str(), errorMsg.c_str(), usageMsg.c_str());
            return -1;
        }

        if (CheckHelp()) {
            return 0;
        }

        int fileCount;   // At least 1 MDBM file arg is required
        if ((fileCount = parser->optionsData.extra.size()) < 1) {
            errorMsg = "Missing MDBM File name";
            fprintf(ErrorFilePtr, "Missing MDBM File name.  Correct %s", usageMsg.c_str());
        }

        // mcreate-specific option processing

        // Alignment option
        uint64_t align = 0;
        if (parser->optionsData.GetOption(RecAlign, align)) {
            switch (align) {
                case 1:
                    align = 0;
                    break;
                case 2:
                    align = MDBM_ALIGN_16_BITS;
                    break;
                case 4:
                    align = MDBM_ALIGN_32_BITS;
                    break;
                case 8:
                    align = MDBM_ALIGN_64_BITS;
                    break;
                default:
                    fprintf(ErrorFilePtr, "Invalid alignment value %d, ignoring.\n", (int) align);
                    align = 0;
            }
        }

        int flags = MDBM_O_RDWR | MDBM_O_CREAT, cacheModeParam;
        uint64_t pageSize = MDBM_PAGESIZ, preSize, mainDBSize, spillSize;

        if (!parser->GetCreateOptions(flags, cacheModeParam, preSize,
                                      pageSize, mainDBSize, spillSize)) {
            fprintf(ErrorFilePtr, "Unable to process options, %s", usageMsg.c_str());
            return -1;
        }

        string permisMode;    // Get mode
        int mode = 0666;
        if (parser->optionsData.GetOption(FilePermissions, permisMode)) {
            char *s;
            errno = 0;
            mode = strtol(permisMode.c_str(), &s, 0);  // deal with octal numbers
            if (errno) {
                mode = 0666;
                fprintf(ErrorFilePtr, "Invalid mode %s, ignoring\n", permisMode.c_str());
            }
        }

        bool truncateDB = false;   // Do we truncate if exists
        parser->optionsData.GetOption(TruncateDB, truncateDB);
        if (truncateDB) {
            flags |= MDBM_O_TRUNC;
        }

        MDBM *db = NULL;

        for (int i = 0; i < fileCount; ++i) {
            string fullPath(GetFullPath(parser->optionsData.extra[i]));
            if (!(db = OpenDBWithParams(fullPath.c_str(), flags, mode, cacheModeParam,
                                          preSize, pageSize, mainDBSize, spillSize))) {
                continue;
            }
            if (align && mdbm_set_alignment(db, (int) align)) {
                fprintf(ErrorFilePtr, "%s: Cannot set alignment to %d\n", fullPath.c_str(), (int) align);
            }
            mdbm_fsync(db);
            mdbm_close(db);
        }

        return 0;
    }

};

REGISTER_MASH_COMMAND(MdbmCreateCommand)


///  mpreload (mdbm_preload)
class MdbmPreloadCommand : public MdbmCommand {
public:
    MdbmPreloadCommand() :
        MdbmCommand("mpreload", "Preload the pages of an MDBM") {
        parser = new OptionParser();
        parser->AddLockModeOption();
    }

    virtual int Run(const vector<string> &args) {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow) || stopNow)) {
            return ret;
        }

        std::string fullPath;
        MDBM *db = NULL;
        if (!(db = OpenExisting(0,  parser->optionsData.extra[0], fullPath, true))) {
            return -1;
        }
        ret = mdbm_preload(db);
        MdbmData.DropHandle(db);
        return ret;
    }

};
REGISTER_MASH_COMMAND(MdbmPreloadCommand)

///  mdelete_lockfiles (mdbm_delete_lockfiles)

class MdbmDeleteLockfilesCommand : public MdbmSimpleCommand
{
public:
    MdbmDeleteLockfilesCommand() :
        MdbmSimpleCommand("mdelete_lockfiles", "Delete the lock files of an MDBM")
    {
        parser = new OptionParser();   // No special command-line args: use OptionParser
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("deleting lockfiles of"));
    }

protected:
    int workFunc(const char *filename)
    {
        return mdbm_delete_lockfiles(filename);
    }
};

REGISTER_MASH_COMMAND(MdbmDeleteLockfilesCommand)

///  mexport (mdbm_export)

// mexport parser class - constructor defines supported options
class MdbmExportParser : public OptionParser
{
public:
    MdbmExportParser()
    {
        AddLockModeOption();
        AddDataFormatOption();
        RegisterOption(string("o"), OPTION_TYPE_STRING, OutputFile,
                       string("Write to <output-file> instead of standard output"));
        // Allow mexport to have the page size option so we can override the page size
        RegisterOption(string("p"), OPTION_TYPE_SIZE, PageSize,
                       string("Page size (default is the MDBMs page size)"));
        RegisterOption(string("T"), OPTION_TYPE_FLAG, NoDbHeader,
                       string("Input has no Berkeley DB-dump header"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
    }
};

// mexport command class
class MdbmExportCommand : public MdbmCommand
{
public:
    MdbmExportCommand() :
        MdbmCommand("mexport", "Export, in human-readable form, the contents of an MDBM")
    {
        parser = new MdbmExportParser();
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow) || stopNow)) {
            return ret;
        }

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath);
        if (db == NULL) {
            return -1;
        }

        FILE *fp = OutputFilePtr;
        string outputFile;
        if (parser->optionsData.GetOption(OutputFile, outputFile)) {
            string outpath(GetFullPath(outputFile));
            if ((fp = fopen(outpath.c_str(), "w")) == NULL) {
                fprintf(ErrorFilePtr, "Cannot open %s: %s\n", outpath.c_str(), strerror(errno));
                mdbm_close(db);
                return -1;
            }
        }

        setvbuf(fp, NULL, _IOFBF, BUFSIZ * 16);  // Extend buffer to buffer more

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool cdbOutput = (strcasecmp(dataFormat.c_str(), "cdb") == 0);

        // retrieve actual locking mode via API, to handle ANY_LOCKS
        int lockFlags = mdbm_get_lockmode(db);
        // Apply exclusive or shared lock only once for better performance
        // For partition locking, iteration takes care of the locking
        bool doLockingOnce = (lockFlags == 0) || (lockFlags == MDBM_RW_LOCKS);
        if (doLockingOnce && (mdbm_lock_smart(db, NULL, MDBM_O_RDONLY) != 1)) {
            fprintf(ErrorFilePtr, "Unable to lock MDBM %s: %s\n",
                    fullPath.c_str(), strerror(errno));
            MdbmData.DropHandle(db);
            return -1;
        }

        bool noDbHeader = false;
        parser->optionsData.GetOption(NoDbHeader, noDbHeader);
        // Header in Berkeley DBdump format: http://www.sleepycat.com/docs/utility/db_dump.html
        if (!cdbOutput && !noDbHeader) {
            fputs("format=print\n", fp);
            fputs("type=hash\n", fp);
            uint64_t pageSize = 0;
            int dbPageSize = mdbm_get_page_size(db);
            // Allow export to override page size via the -p command line arg
            if (parser->optionsData.GetOption(PageSize, pageSize) && pageSize) {
                dbPageSize = (int) pageSize;
            }
            fprintf(fp, "mdbm_pagesize=%d\n", dbPageSize);
            fprintf(fp, "mdbm_pagecount=%d\n", (int)(mdbm_get_size(db) / dbPageSize));
            fputs("HEADER=END\n", fp);
        }

        kvpair kv;
        for (kv = mdbm_first(db); kv.key.dptr != NULL; kv = mdbm_next(db)) {
            if (cdbOutput) {     // Use cdbmake/cdbdump format: http://cr.yp.to/cdb/cdbmake.html
                mdbm_cdbdump_to_file(kv, fp);
            } else {
                mdbm_dbdump_to_file(kv, fp);
            }
        }

        if (fp != stdout) {
            if (cdbOutput) {
                mdbm_cdbdump_trailer_and_close(fp);
            } else {
                mdbm_dbdump_trailer_and_close(fp);
            }
        } else if (cdbOutput) {   // Don't close stdout
            // Add newline at the end of output
            // can't use mdbm_cdbdump_trailer_and_close because it closes stdout
            fputc('\n', fp);
        }

        if (doLockingOnce && (mdbm_unlock_smart(db, NULL, MDBM_O_RDONLY) != 1)) {
            fprintf(ErrorFilePtr, "Unable to unlock MDBM %s: %s\n",
                    fullPath.c_str(), strerror(errno));
        }

        MdbmData.DropHandle(db);
        return 0;
    }

};

REGISTER_MASH_COMMAND(MdbmExportCommand)


///  mimport (mdbm_import)

// mimport parser class - constructor defines supported options
class MdbmImportParser : public MdbmOptionParser
{
public:
    MdbmImportParser()
    {
        AddDataFormatOption();
        RegisterOption(string("e"), OPTION_TYPE_FLAG, EraseZeroLen,
                       string("Erase zero-length values"));
        RegisterOption(string("i"), OPTION_TYPE_STRING, InputFile,
                       string("Read from named input file instead of standard input"));
        RegisterOption(string("S"), OPTION_TYPE_UNSIGNED, StoreMode,
                       string("Store Mode Value:\n"
"\t\t0 - MDBM_INSERT     - Store fails for existing key\n"
"\t\t1 - MDBM_REPLACE    - Replace entry of existing key, else insert (default)\n"
"\t\t2 - MDBM_INSERT_DUP - Insert duplicate entries for existing keys\n"
"\t\t3 - MDBM_MODIFY     - Store fails if added entry does not already exist"));
        SetMaxAllowed(StoreMode, 3);
        RegisterOption(string("T"), OPTION_TYPE_FLAG, NoDbHeader,
                       string("Input has no Berkeley DB-dump header"));
        RegisterOption(string("Z"), OPTION_TYPE_FLAG, TruncateDB,
                       string("Truncate DB if already exists"));
    }
};

// mimport command implementation
class MdbmImportCommand : public MdbmCommand
{
public:
    MdbmImportCommand() :
        MdbmCommand("mimport", "Read key/value data and import into an MDBM")
    {
        parser = new MdbmImportParser();
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow)) || stopNow) {
            return ret;
        }

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool cdbOutput = (strcasecmp(dataFormat.c_str(), "cdb") == 0);

        bool noDbHeader = false;
        parser->optionsData.GetOption(NoDbHeader, noDbHeader);

        FILE *fp;
        string inputFile;
        if (parser->optionsData.GetOption(InputFile, inputFile)) {
            string inpath(GetFullPath(inputFile));
            if ((fp = fopen(inpath.c_str(), "r")) == NULL) {
                fprintf(ErrorFilePtr, "Cannot open %s: %s\n", inpath.c_str(), strerror(errno));
                return -1;
            }
        } else {  // Using stdin won't work until pipes are implemented
            fp = stdin;
            fprintf(ErrorFilePtr, "Using stdin is not supported yet\n");
            return -1;
        }

        uint32_t linenum = 1;
        uint64_t pageSize = MDBM_PAGESIZ;
        if (!cdbOutput && !noDbHeader) {   // DBdump format may have a header with page size
            int hdrPageSize = 0, pageCount, largeObjSupport;
            mdbm_dbdump_import_header(fp, &hdrPageSize, &pageCount, &largeObjSupport, &linenum);
            if (hdrPageSize) {
                pageSize = hdrPageSize;
            }
        }

        int flags = 0;

        bool truncateDB = false;
        parser->optionsData.GetOption(TruncateDB, truncateDB);
        if (truncateDB) {
            flags |= MDBM_O_TRUNC;
        }

        MDBM *db = NULL;
        if (!(db = CreateMdbm(flags, 0666, pageSize))) {
            fprintf(ErrorFilePtr, "Failed to open MDBM for import: %s\n", strerror(errno));
        }

        uint64_t storeFlag = MDBM_REPLACE;
        parser->optionsData.GetOption(StoreMode, storeFlag);

        ret = 0;
        if (cdbOutput) {
            ret = mdbm_cdbdump_import(db, fp, inputFile.c_str(), storeFlag);
        } else {
            ret = mdbm_dbdump_import(db, fp, inputFile.c_str(), storeFlag, &linenum);
        }

        if (ret) {
            fprintf(ErrorFilePtr, "Failed to import, returned %d: %s\n", ret, strerror(errno));
        }

        mdbm_fsync(db);
        mdbm_close(db);
        fclose(fp);

        return 0;
    }

};

REGISTER_MASH_COMMAND(MdbmImportCommand)

///  mcompress (mdbm_compress)

// mcompress command implementation
class MdbmCompressCommand : public MdbmSimpleCommand
{
public:
    MdbmCompressCommand() :
        MdbmSimpleCommand("mcompress", "Reduce the file size of an MDBM")
    {
        parser = new OptionParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("compressing"));
    }


protected:
    int workFunc(const char *filename)
    {
        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath, false);
        if (db == NULL) {
            fprintf(ErrorFilePtr, "Cannot open %s when compressing\n", filename);
            return -1;
        }
        mdbm_compress_tree(db);
        MdbmData.DropHandle(db);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmCompressCommand)

///  mcheck (mdbm_check)

// mcheck parser class - constructor defines supported options
class MdbmCheckParser : public OptionParser
{
public:
    MdbmCheckParser()
    {
        AddLockModeOption();

        RegisterOption(string("I"), OPTION_TYPE_UNSIGNED, IntegrityCheckLevel,
                       string("Database intergrity check level (0-4, default: 3)\n"
"\t\t0 - no header checks (invoke page-level checking only)\n"
"\t\t1 - check header, chunks (list of free pages and allocated pages)\n"
"\t\t2 - check directory pages\n"
"\t\t3 - check data pages and large objects"));
        SetMaxAllowed(IntegrityCheckLevel, 4);
        RegisterOption(string("P"), OPTION_TYPE_SIGNED, PageNumber,
                       string("Check specified page (-1 means check all pages)"));
        RegisterOption(string("M"), OPTION_TYPE_FLAG, ShowMdbmVersion,
                       string("Display MDBM file version\n"
"\t\tThis option may only be used with the -v option.\n"
"\t\tUse '-v 0' to return just the unadorned version number"));
        RegisterOption(string("v"), OPTION_TYPE_UNSIGNED, VerbosityLevel,
                       string("Set verbosity level"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
        RegisterOption(string("X"), OPTION_TYPE_UNSIGNED, VerifyVersion,
                       string("Verify that MDBM file version matches given number"));
        SetMaxAllowed(VerifyVersion, 3);
    }
};

// mcheck command implementation
class MdbmCheckCommand : public MdbmCommand
{
public:
    MdbmCheckCommand() :
        MdbmCommand("mcheck", "Verify MDBM Integrity")
    {
        parser = new MdbmCheckParser();
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow)) || stopNow) {
            return ret;
        }

        uint64_t checkLevel = 3;
        parser->optionsData.GetOption(IntegrityCheckLevel, checkLevel);
        int64_t pageNum = -1;
        parser->optionsData.GetOption(PageNumber, pageNum);
        bool showVersion = false;
        parser->optionsData.GetOption(ShowMdbmVersion, showVersion);
        uint64_t verbosity = 0;
        parser->optionsData.GetOption(VerbosityLevel, verbosity);
        uint64_t expectedVersion = 0;
        parser->optionsData.GetOption(VerifyVersion, expectedVersion);

        mdbm_log_minlevel(LOG_INFO);

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath);
        if (db == NULL) {
            return -1;
        }

        if (expectedVersion) {
            uint32_t version = mdbm_get_version(db);
            if (version != (uint32_t) expectedVersion) {
                fprintf(ErrorFilePtr, "MDBM versions do not match, expected version=%u"
                                ", actual version=%u\n",
                        (uint32_t) expectedVersion, version);
                MdbmData.DropHandle(db);
                return(-1);
            }
        }

        if (showVersion) {   // Show version means don't perform checks
            uint32_t version = mdbm_get_version(db);
            fprintf(OutputFilePtr, "%s%d\n", (verbosity ? "MDBM version: " : ""), version);
            mdbm_close(db);
            return 0;
        }

        if (!checkLevel) {   // Integrity-check level zero means only check data pages and return
            if (pageNum < 0) {
                ret = mdbm_chk_all_page(db);
            } else {
                ret = mdbm_chk_page(db, (int) pageNum);
            }
            MdbmData.DropHandle(db);
            return ret;
        }

        if (mdbm_lock_smart(db, NULL, MDBM_O_RDONLY) != 1) {
            fprintf(ErrorFilePtr, "%s: Unable to lock MDBM: %s\n", fullPath.c_str(), strerror(errno));
            MdbmData.DropHandle(db);
            return -1;
        }

        ret = mdbm_check(db, checkLevel, (int) verbosity);
        if (ret && verbosity) {
            fprintf(ErrorFilePtr, "%s: failed integrity check\n", fullPath.c_str());
        }

        if (mdbm_unlock_smart(db, NULL, MDBM_O_RDONLY) != 1) {
            fprintf(ErrorFilePtr, "Unable to unlock MDBM %s: %s\n", fullPath.c_str(), strerror(errno));
        }

        MdbmData.DropHandle(db);
        return ret;
    }

};

REGISTER_MASH_COMMAND(MdbmCheckCommand)

///  mcompare (mdbm_compare)

// mcompare parser class
class MdbmCompareParser : public OptionParser
{
public:
    MdbmCompareParser()
    {
        AddLockModeOption();
        AddDataFormatOption();
        RegisterOption(string("c"), OPTION_TYPE_UNSIGNED, CompareType,
                       string("Type of comparison to perform\n"
"\t\t0 - Missing entries only: show entries not in 2nd DB\n"
"\t\t1 - Show entries that are different but exist in both DBs"));
        SetMaxAllowed(CompareType, 1);
        RegisterOption(string("o"), OPTION_TYPE_STRING, OutputFile,
                       string("Write to <output-file> instead of standard output"));
        RegisterOption(string("k"), OPTION_TYPE_FLAG, PrintKeys, string("Print key data"));
        RegisterOption(string("v"), OPTION_TYPE_FLAG, VerbosityLevel, string("Verbose output"));
        RegisterOption(string("V"), OPTION_TYPE_FLAG, PrintValues, string("Print value data"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
    }
};

// mcheck command implementation
class MdbmCompareCommand : public MdbmCommand
{
public:
    MdbmCompareCommand() :
        MdbmCommand("mcompare", "Compare two MDBMs")
    {
        parser = new MdbmCompareParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] File1.mdbm File2.mdbm\n");
    }

    // print a key-value pair using the following format:
    //  1. In cdb format, or in
    //  2. "xxd" format using PrintDatum(), based on values of printKey and printVal
    void printKV(FILE *outfile, datum key, datum val, bool cdbOutput, bool printKey, bool printVal)
    {
        if (cdbOutput) {
            kvpair kvp = {key, val};
            mdbm_cdbdump_to_file(kvp, outfile);
            return;   // CDB output format always prints key and value
        }
        if (printKey) {
            PrintDatum(outfile, "  K ", key);
        }
        if (printVal) {
            PrintDatum(outfile, "  V ", val);
        }
    }

    // Returns a count of qualifying items that are missingOnly and/or differentOnly
    uint64_t printDifferent(MDBM* db1, MDBM* db2, FILE *outfile,
                            bool missingOnly, bool differentOnly,
                            bool cdbOutput, bool printKey, bool printVal)
    {
        uint64_t counter = 0;
        kvpair kv;
        datum d;
        MDBM_ITER iter;

        MDBM_ITER_INIT(&iter);
        kv = mdbm_first_r(db1, &iter);
        while (kv.key.dptr != NULL) {
            d = mdbm_fetch(db2, kv.key);

            if (d.dptr == NULL) {
                if (!differentOnly) {
                    printKV(outfile, kv.key, kv.val, cdbOutput, printKey, printVal);
                    ++counter;
                }
            } else if ((kv.val.dsize != d.dsize) || memcmp(kv.val.dptr, d.dptr, kv.val.dsize)) {
                if (!missingOnly) {
                    printKV(outfile, kv.key, kv.val, cdbOutput, printKey, printVal);
                    ++counter;
                }
            }
            kv = mdbm_next_r(db1, &iter);
        }
        return counter;
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 2)) || stopNow) {
            return ret;
        }

        FILE *fp = stdout;
        bool needToClose = false;    // Whether we need to close at the end
        if (OutputFilePtr) {
            fp = OutputFilePtr;   // Don't need to close at the end
        } else {
            string outputFile;
            if (parser->optionsData.GetOption(OutputFile, outputFile)) {
                string outpath(GetFullPath(outputFile));
                if ((fp = fopen(outpath.c_str(), "w")) == NULL) {
                    fprintf(ErrorFilePtr, "Cannot open %s: %s\n", outpath.c_str(), strerror(errno));
                    return -1;
                }
                needToClose = true;
            }
        }

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool cdbOutput = (strcasecmp(dataFormat.c_str(), "cdb") == 0);

        bool printKey = false;
        parser->optionsData.GetOption(PrintKeys, printKey);
        bool printVal = false;
        parser->optionsData.GetOption(PrintValues, printVal);

        const uint64_t PRINT_ALL_DIFFERENCES = 10;
        uint64_t diffCount, compareType = PRINT_ALL_DIFFERENCES;
        parser->optionsData.GetOption(CompareType, compareType);

        string fullPath1, fullPath2;
        MDBM *db1 = OpenExisting(0, parser->optionsData.extra[0], fullPath1);
        if (db1 == NULL) {
            return -1;
        }
        MDBM *db2 = OpenExisting(0, parser->optionsData.extra[1], fullPath2);
        if (db2 == NULL) {
            mdbm_close(db1);
            return -1;
        }

        if (compareType == 0) {          // Print only the missing entries
            diffCount = printDifferent(db1, db2, fp, true, false, cdbOutput, printKey, printVal);
        } else if (compareType == 1) {   // Print only the different entries
            diffCount = printDifferent(db1, db2, fp, false, true, cdbOutput, printKey, printVal);
        } else {                         // compareType == PRINT_ALL_DIFFERENCES
            // What's different in db1
            diffCount = printDifferent(db1, db2, fp, false, false, cdbOutput, printKey, printVal);
            // Now pick up what's only in db2
            diffCount += printDifferent(db2, db1, fp, true, false, cdbOutput, printKey, printVal);
        }

        bool verbosity = false;
        parser->optionsData.GetOption(VerbosityLevel, verbosity);

        if (verbosity) {
            if (diffCount == 0) {
                fprintf(fp,"Files %s and %s are the same\n", fullPath1.c_str(), fullPath2.c_str());
            } else {
                fprintf(fp,"Files %s and %s are different\n", fullPath1.c_str(), fullPath2.c_str());
            }
        }

        if (needToClose) {
            fclose(fp);
        }
        MdbmData.DropHandle(db1);
        MdbmData.DropHandle(db2);

        return (diffCount != 0) ? 1 : 0;
    }
};

REGISTER_MASH_COMMAND(MdbmCompareCommand)

///  mcopy (mdbm_copy)

// mcopy parser: allow lock mode specification
class MdbmCopyParser : public OptionParser
{
public:
    MdbmCopyParser()
    {
        AddLockModeOption();
    }
};

// mcopy command implementation
class MdbmCopyCommand : public MdbmCommand
{
public:
    MdbmCopyCommand() :
        MdbmCommand("mcopy", "Copy an MDBM")
    {
        parser = new MdbmCopyParser();
    }

    // Always requires 2 arguments: source and destination
    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] SOURCE.mdbm DESTINATION.mdbm\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 2)) || stopNow) {
            return ret;
        }

        int lockFlags = parser->GetLockFlags();  // Default: whichever locking is currently used

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath);
        if (db == NULL) {
            return -1;
        }

        int fd = 0;
        string destPath = GetFullPath(parser->optionsData.extra[1]);
        if ((fd = open(destPath.c_str() ,O_RDWR | O_CREAT | O_TRUNC, 0666)) < 0) {
            fprintf(ErrorFilePtr, "Cannot open copy destination %s: %s\n",
                    destPath.c_str(), strerror(errno));
            MdbmData.DropHandle(db);
            return -1;
        }

        ret = 0;
        const int MAX_TRIES = 3;   // Try to copy 3 times, otherwise fail
        // Lock entire MDBM if locking mode is exclusive locking (when lockFlags are zero)
        if (mdbm_internal_fcopy(db, fd,(lockFlags == 0) ? MDBM_COPY_LOCK_ALL : 0, MAX_TRIES) < 0) {
            fprintf(ErrorFilePtr, "Cannot copy to destination %s: %s\n",
                    destPath.c_str(), strerror(errno));
            ret = -1;
        }

        // The current working file should be the copy-to file
        MdbmData.SetCurrentMdbmFile(destPath);
        close(fd);
        MdbmData.DropHandle(db);
        return ret;
    }
};

REGISTER_MASH_COMMAND(MdbmCopyCommand)

///  mfetch (mdbm_fetch)

// mfetch parser class
class MdbmFetchParser : public OptionParser
{
public:
    MdbmFetchParser()
    {
        AddLockModeOption();
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
        RegisterOption(string("v"), OPTION_TYPE_FLAG, VerbosityLevel, string("Verbose output"));
        RegisterOption(string("P"), OPTION_TYPE_FLAG, PageNumber, string("Print page number"));
        RegisterOption(string("o"), OPTION_TYPE_STRING, OutputFile,
                       string("Write to <output-file> instead of standard output"));
        RegisterOption(string("F"), OPTION_TYPE_STRING, DataFormat,
                       string("Data format:\n"
"                cdb     - CDB data format (output only)\n"
"                dbdump  - DBdump data format (output only)\n"
"                hex     - Input/output unprintable characters as 2 hex characters as \\x##"));
        set<string> formats;
        formats.insert("cdb");
        formats.insert("dbdump");
        formats.insert("hex");
        SetAllowedValues(DataFormat, formats);
    }
};

// mfetch command implementation
class MdbmFetchCommand : public MdbmCommand
{
public:
    MdbmFetchCommand() :
        MdbmCommand("mfetch", "Fetch value of keys from an MDBM")
    {
        parser = new MdbmFetchParser();
    }

    // Always requires 2 arguments: MDBM name and key to fetch
    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] <MDBM> Key1 Key2 ...\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 2, true)) || stopNow) {
            return ret;
        }

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath, true);
        if (db == NULL) {
            return -1;
        }

        bool verbosity = false;
        parser->optionsData.GetOption(VerbosityLevel, verbosity);
        bool printPageNum = false;
        parser->optionsData.GetOption(PageNumber, printPageNum);

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool chars2hex = (strcasecmp(dataFormat.c_str(), "hex") == 0);
        bool cdbOutput = (strcasecmp(dataFormat.c_str(), "cdb") == 0);
        bool dbdumpOutput = (strcasecmp(dataFormat.c_str(), "dbdump") == 0);

        FILE *fp = NULL;
        string outputFile;
        if ((cdbOutput || dbdumpOutput) && parser->optionsData.GetOption(OutputFile, outputFile)) {
            string outpath(GetFullPath(outputFile));
            if ((fp = fopen(outpath.c_str(), "w")) == NULL) {
                fprintf(ErrorFilePtr, "Cannot open %s: %s\n", outpath.c_str(), strerror(errno));
            }
        }

        datum key, val;
        MDBM_ITER iter;

        for(unsigned int i = 1; i < parser->optionsData.extra.size(); ++i) {
            string ky = parser->optionsData.extra[i];
            if (chars2hex) {
                ky = GetStringFromHex(ky);
            }
            key.dptr = (char *) ky.c_str();
            key.dsize = ky.size();

            if (mdbm_lock_smart(db, &key, MDBM_O_RDONLY) != 1) {
                fprintf(ErrorFilePtr, "%s: Unable to lock MDBM: %s\n",
                        fullPath.c_str(), strerror(errno));
                MdbmData.DropHandle(db);
                return -1;
            }

            MDBM_ITER_INIT( &iter );
            while (mdbm_fetch_dup_r( db, &key, &val, &iter ) == 0) {
                if (fp && cdbOutput) {
                    kvpair kvp = {key, val};
                    mdbm_cdbdump_to_file(kvp, fp);
                }
                if (fp && dbdumpOutput) {
                    kvpair kvp = {key, val};
                    mdbm_dbdump_to_file(kvp, fp);
                }
                bool freeNeeded = ConvertToHexFormat(key, val, chars2hex);
                if (verbosity) {
                    fprintf(OutputFilePtr,
                            "key (%4d bytes): %.*s\n", key.dsize, key.dsize, key.dptr);
                    fprintf(OutputFilePtr,
                            "val (%4d bytes): %.*s\n", val.dsize, val.dsize, val.dptr);
                    if (printPageNum) {
                        fprintf(OutputFilePtr, "Page-number is: %u\n", mdbm_get_page(db, &key));
                    }
                } else {
                    fwrite(key.dptr, key.dsize, 1, OutputFilePtr);
                    fputs(" :: ", OutputFilePtr);
                    fwrite(val.dptr, val.dsize, 1, OutputFilePtr);
                    if (printPageNum) {
                        fputs(" :: ", OutputFilePtr);
                        fprintf(OutputFilePtr, "page-number= %u", mdbm_get_page(db, &key));
                    }
                    fputc('\n', OutputFilePtr);
                }
                if (freeNeeded) {
                    free(key.dptr);
                    free(val.dptr);
                }
            }
            mdbm_unlock_smart(db, &key, MDBM_O_RDONLY);
        }

        MdbmData.DropHandle(db);
        if (fp && cdbOutput) {
            mdbm_cdbdump_trailer_and_close(fp);
        }
        if (fp && dbdumpOutput) {
            mdbm_dbdump_trailer_and_close(fp);
        }
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmFetchCommand)

///  mstore - store a key-value pair in an MDBM

// mstore parser class
class MdbmStoreParser : public OptionParser
{
public:
    MdbmStoreParser()
    {
        AddLockModeOption();
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
        RegisterOption(string("S"), OPTION_TYPE_UNSIGNED, StoreMode,
                       string("Store Mode Value:\n"
"\t\t0 - MDBM_INSERT     - Store fails for existing key\n"
"\t\t1 - MDBM_REPLACE    - Replace entry of existing key, else insert (default)\n"
"\t\t2 - MDBM_INSERT_DUP - Insert duplicate entries for existing keys\n"
"\t\t3 - MDBM_MODIFY     - Store fails if added entry does not already exist"));
        SetMaxAllowed(StoreMode, 3);
        RegisterOption(string("O"), OPTION_TYPE_FLAG, LargeObj,
                       string("Enable large object support"));
        RegisterOption(string("P"), OPTION_TYPE_FLAG, PageNumber, string("Print page number"));
        RegisterOption(string("F"), OPTION_TYPE_STRING, DataFormat,
                       string("Data format:\n"
"                hex     - Input/output unprintable characters as 2 hex characters as \\x##"));
        set<string> formats;
        formats.insert("hex");
        SetAllowedValues(DataFormat, formats);
    }
};

// mstore command implementation
class MdbmStoreCommand : public MdbmCommand
{
public:
    MdbmStoreCommand() :
        MdbmCommand("mstore", "Store key/value pair into an MDBM")
    {
        parser = new MdbmStoreParser();
    }

    // Always requires 3 arguments: MDBM name, the key, and the value
    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] <MDBM> Key Value\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 3)) || stopNow) {
            return ret;
        }

        int flags = 0;
        bool largeObj = false;   // Large object flag
        parser->optionsData.GetOption(LargeObj, largeObj);
        if (largeObj) {
            flags |= MDBM_LARGE_OBJECTS;
        }

        string fullPath;
        MDBM *db = OpenExisting(flags, parser->optionsData.extra[0],
                                          fullPath, false);
        if (db == NULL) {
            return -1;
        }

        bool printPageNum = false;
        parser->optionsData.GetOption(PageNumber, printPageNum);

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool chars2hex = (strcasecmp(dataFormat.c_str(), "hex") == 0);

        string kyStr = parser->optionsData.extra[1];
        string valueStr = parser->optionsData.extra[2];
        if (chars2hex) {
            kyStr = GetStringFromHex(kyStr);
            valueStr = GetStringFromHex(valueStr);
        }
        datum key, val;
        key.dptr = (char *) kyStr.c_str();
        key.dsize = kyStr.size();
        val.dptr = (char *) valueStr.c_str();
        val.dsize = valueStr.size();

        uint64_t storeFlag = MDBM_REPLACE;
        parser->optionsData.GetOption(StoreMode, storeFlag);

        if (mdbm_lock_smart(db, &key, MDBM_O_RDWR) != 1) {
            fprintf(ErrorFilePtr, "%s: Unable to lock MDBM: %s\n", fullPath.c_str(), strerror(errno));
            MdbmData.DropHandle(db);
            return -1;
        }

        if ((ret = mdbm_store(db, key, val, storeFlag)) < 0) {  // Perform store
            string err(strerror(errno));
            // MODIFY returns ENOENT if not found, which strerror converts to
            // "No such file or directory", which is highly confusing.
            if (errno == ENOENT) {
                err = "Missing entry";
            }
            fprintf(ErrorFilePtr, "%s: Unable to store successfully: %s\n",
                    fullPath.c_str(), err.c_str());
        } else if (ret == 1) {   // MDBM_INSERT can return 1 if entry exists
            fprintf(ErrorFilePtr, "%s: Unable to insert existing key: %s\n", fullPath.c_str(), key.dptr);
        } else if (printPageNum) {
            fprintf(OutputFilePtr, "Stored in page number %u\n", mdbm_get_page(db, &key));
        }

        mdbm_unlock_smart(db, &key, 0);
        MdbmData.DropHandle(db);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmStoreCommand)

///  mlock_print (mdbm_lock when used with the -d option): print lock information

// mlock_print command implementation done in workFunc()
class MdbmLockPrintCommand : public MdbmSimpleCommand
{
public:
    MdbmLockPrintCommand() :
        MdbmSimpleCommand("mlock_print", "Print the lock state of an MDBM")
    {
        parser = new OptionParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("printing the lock state"));
    }


protected:
    int workFunc(const char *filename)
    {
        struct mdbm_locks* locks;
        int needCheck = 0;

        if ((locks = open_locks_inner(filename, MDBM_ANY_LOCKS, 0, &needCheck)) == NULL) {
            fprintf(ErrorFilePtr, "%s: could not open locks\n", filename);
            return -1;
        }
        if (needCheck) {
            fprintf(ErrorFilePtr, "%s: previous owner died, run mcheck\n", filename);
        }
        print_lock_state_inner(locks);
        close_locks_inner(locks);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmLockPrintCommand)

///  mlock_reset

// call mdbm_lock_reset to reset the lock, just like mdbm_lock when used with the -r option.
// mlock_print command implementation done in workFunc().
class MdbmLockResetCommand : public MdbmSimpleCommand
{
public:
    MdbmLockResetCommand() :
        MdbmSimpleCommand("mlock_reset", "Reset the lock of an MDBM")
    {
        parser = new OptionParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("resetting the lock state"));
    }


protected:
    int workFunc(const char *filename)
    {
        return mdbm_lock_reset(filename, 0);
    }
};

REGISTER_MASH_COMMAND(MdbmLockResetCommand)


///  mlock - lock an MDBM

// mlock parser class: supports locking mode and read-only specifier flag
class MdbmLockParser : public OptionParser
{
public:
    MdbmLockParser()
    {
        AddLockModeOption();
        RegisterOption(string("r"), OPTION_TYPE_FLAG, ReadOnlyAccess,
                       string("Lock for Read-only access"));
    }
};

// mlock command implementation
class MdbmLockCommand : public MdbmCommand
{
public:
    MdbmLockCommand() :
        MdbmCommand("mlock", "Lock an MDBM")
    {
        parser = new MdbmLockParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] FileToLock.mdbm\n");
    }

    // Decided mlock always requires at least 1 arg, no implicit MDBM
    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 1)) || stopNow) {
            return ret;
        }

        bool readOnly = false;
        parser->optionsData.GetOption(ReadOnlyAccess, readOnly);

        string filename(parser->optionsData.extra[0]);
        string fullPath(GetFullPath(filename));

        MDBM *currentWorkingMdbm = OpenExisting(0, filename, fullPath, readOnly);
        if (currentWorkingMdbm == NULL) {
            return -1;
        }
        int flags = (readOnly ? MDBM_O_RDONLY : MDBM_O_RDWR);
        if (mdbm_lock_smart(currentWorkingMdbm, NULL, flags) != 1) {
            fprintf(ErrorFilePtr, "%s: Unable to lock %s\n",
                    mdbm_get_filename(currentWorkingMdbm), strerror(errno));
            return -1;
        }
        MdbmData.LockHandle(currentWorkingMdbm);
        MdbmData.SetCurrentMdbmFile(fullPath);
        MdbmData.DropHandle(currentWorkingMdbm);

        return 0;
    }

};

REGISTER_MASH_COMMAND(MdbmLockCommand)

///  munlock - unlock the MDBM that is currently locked

class MdbmUnlockParser : public OptionParser
{
public:
    MdbmUnlockParser()
    {
        AddLockModeOption();  // Can specify lock mode
    }
};

class MdbmUnlockCommand : public MdbmCommand
{
public:
    MdbmUnlockCommand() :
        MdbmCommand("munlock", "Unlock an MDBM")
    {
        parser = new MdbmUnlockParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] FileToLock.mdbm\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 1)) || stopNow) {
            return ret;
        }

        string filename(parser->optionsData.extra[0]);
        string fullPath(GetFullPath(filename));

        // Do not allow unlocking of an MDBM that's hasn't been locked
        if (!MdbmData.UnlockHandle(fullPath)) {
            fprintf(ErrorFilePtr, "Cannot unlock %s since it is not locked\n", fullPath.c_str());
            return -1;
        }

        MDBM *db = MdbmData.FindHandle(fullPath);
        if (db == NULL) {    // This should never happen because UnlockHandle should fail first
            fprintf(ErrorFilePtr, "Cannot find open handle for %s\n", fullPath.c_str());
            return -1;
        }

        if (mdbm_unlock_smart(db, NULL, 0) != 1) {
            fprintf(ErrorFilePtr, "%s: Unable to unlock: %s\n", fullPath.c_str(), strerror(errno));
            return -1;
        }

        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmUnlockCommand)

///  mpurge - purge the contents of an MDBM w/o losing config

// mpurge parser class
class MdbmPurgeParser : public OptionParser
{
public:
    MdbmPurgeParser()
    {
        AddLockModeOption();
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
    }
};

// mpurge implementation - overloads MdbmSimpleCommand, so workFunc() does the work
class MdbmPurgeCommand : public MdbmSimpleCommand
{
public:
    MdbmPurgeCommand() :
        MdbmSimpleCommand("mpurge", "Purge the contents of an MDBM")
    {
        parser = new MdbmPurgeParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("purging"));
    }


protected:
    int workFunc(const char *filename)
    {
        string fullPath;
        MDBM *db = OpenExisting(0, filename, fullPath, false);
        if (db == NULL) {
            return -1;
        }
        mdbm_purge(db);
        MdbmData.DropHandle(db);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmPurgeCommand)

///  msync

class MdbmSyncParser : public OptionParser
{
public:
    MdbmSyncParser()
    {
        AddLockModeOption();
    }
};

// msync implementation - overloads MdbmSimpleCommand, so workFunc() does the work
class MdbmSyncCommand : public MdbmSimpleCommand
{
public:
    MdbmSyncCommand() :
        MdbmSimpleCommand("msync", "Sync changes to an MDBM to disk")
    {
        parser = new MdbmSyncParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("syncing"));
    }


protected:
    int workFunc(const char *filename)
    {
        string fullPath;
        MDBM *db = OpenExisting(0, filename, fullPath);
        if (db == NULL) {
            return -1;
        }
        int ret = mdbm_sync(db);
        if (ret) {
            fprintf(ErrorFilePtr, "%s: mdbm_sync failed: %s\n", filename, strerror(errno));
        }
        MdbmData.DropHandle(db);
        return ret;
    }
};

REGISTER_MASH_COMMAND(MdbmSyncCommand)

///  mopen - open an existing MDBM

// mopen parser class: supports -x async/fsync for MDBM_O_FSYNC/ASYNC flag support
class MdbmOpenParser : public OptionParser
{
public:
    MdbmOpenParser()
    {
        AddLockModeOption();
        RegisterOption(string("r"), OPTION_TYPE_FLAG, ReadOnlyAccess,
                       string("Open for Read-only access"));
        RegisterOption(string("O"), OPTION_TYPE_FLAG, LargeObj,
                       string("Enable large object support"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
        RegisterOption(string("x"), OPTION_TYPE_STRING, ExtraData,
                       string("Extra Open Flags:\n"
"                async     - Enable asynchronous writes\n"
"                fsync     - Enable syncing MDBM file on close"));
        set<string> extraFlags;
        extraFlags.insert("async");
        extraFlags.insert("fsync");
        SetAllowedValues(ExtraData, extraFlags);
        RegisterOption(string("Z"), OPTION_TYPE_FLAG, TruncateDB, string("Truncate DB"));
    }
};

// mopen implementation
class MdbmOpenCommand : public MdbmCommand
{
public:
    MdbmOpenCommand() :
        MdbmCommand("mopen", "Open an MDBM")
    {
        parser = new MdbmOpenParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] FileToOpen.mdbm\n");
    }

    // mopen always requires at least 1 arg, no implicit MDBM
    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 1)) || stopNow) {
            return ret;
        }

        bool readOnly = false;
        parser->optionsData.GetOption(ReadOnlyAccess, readOnly);

        int flags = 0;
        bool truncateDB = false;
        parser->optionsData.GetOption(TruncateDB, truncateDB);
        if (truncateDB) {
            flags |= MDBM_O_TRUNC;
        }

        bool largeObj = false;
        parser->optionsData.GetOption(LargeObj, largeObj);
        if (largeObj) {
            flags |= MDBM_LARGE_OBJECTS;
        }

        string extraFlags;
        parser->optionsData.GetOption(ExtraData, extraFlags);
        if (strcasecmp(extraFlags.c_str(), "async") == 0) {
            flags |= MDBM_O_ASYNC;
        } else if (strcasecmp(extraFlags.c_str(), "fsync") == 0) {
            flags |= MDBM_O_FSYNC;
        }

        string filename(parser->optionsData.extra[0]);
        string fullPath(GetFullPath(filename));

        MDBM *db = OpenExisting(flags, filename, fullPath, readOnly);
        if (db == NULL) {
            return -1;
        }
        MdbmData.SetCurrentMdbmFile(fullPath);

        return 0;
    }

};

REGISTER_MASH_COMMAND(MdbmOpenCommand)

///  mclose

// mclose parser class
class MdbmCloseParser : public OptionParser
{
public:
    MdbmCloseParser()
    {
        RegisterOption(string("f"), OPTION_TYPE_FLAG, Force,
                       string("Force close (disregard open/lock state)"));
    }
};
 
class MdbmCloseCommand : public MdbmSimpleCommand
{
public:
    MdbmCloseCommand() :
        MdbmSimpleCommand("mclose", "Close previously opened MDBM")
    {
        parser = new MdbmCloseParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("closing"));
    }


protected:
    int workFunc(const char *filename)
    {
        string fullPath(GetFullPath(filename));
        bool force = false;
        parser->optionsData.GetOption(Force, force);
        MdbmData.DropName(fullPath, force);   // mclose -f will always close ignoring ref/lockCount
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmCloseCommand)

///  mdelete - delete a key/value pair from an MDBM

// mdelete parser class
class MdbmDeleteParser : public OptionParser
{
public:
    MdbmDeleteParser()
    {
        AddLockModeOption();
        RegisterOption(string("F"), OPTION_TYPE_STRING, DataFormat,
                       string("Data format:\n"
"                hex     - Allow unprintable characters as 2 hex characters as \\x##"));
        set<string> formats;
        formats.insert("hex");
        SetAllowedValues(DataFormat, formats);
        RegisterOption(string("P"), OPTION_TYPE_FLAG, PageNumber, string("Print page number"));
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
    }
};

// mdelete implementation
class MdbmDeleteCommand : public MdbmCommand
{
public:
    MdbmDeleteCommand() :
        MdbmCommand("mdelete", "Delete key/value pairs from an MDBM")
    {
        parser = new MdbmDeleteParser();
    }

    // Always requires at least 2 arguments: MDBM name and key(s) to delete
    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] <MDBM> DeleteKey1 DeleteKey2 ...\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 2, true)) || stopNow) {
            return ret;
        }

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath, false);
        if (db == NULL) {
            return -1;
        }

        bool printPageNum = false;
        parser->optionsData.GetOption(PageNumber, printPageNum);

        string dataFormat;
        parser->optionsData.GetOption(DataFormat, dataFormat);
        bool chars2hex = (strcasecmp(dataFormat.c_str(), "hex") == 0);

        datum key;
        for(unsigned int i = 1; i < parser->optionsData.extra.size(); ++i) {
            string ky = parser->optionsData.extra[i];
            if (chars2hex) {
                ky = GetStringFromHex(ky);
            }
            key.dptr = (char *) ky.c_str();
            key.dsize = ky.size();

            if (mdbm_lock_smart(db, &key, MDBM_O_RDWR) != 1) {
                fprintf(ErrorFilePtr, "%s: Unable to lock MDBM: %s\n",
                        fullPath.c_str(), strerror(errno));
                MdbmData.DropHandle(db);
                return -1;
            }

            unsigned int pageNum = mdbm_get_page(db, &key);
            if (mdbm_delete(db, key) != 0) {
                fprintf(ErrorFilePtr, "%s: Unable to delete key %s: %s\n",
                        fullPath.c_str(), ky.c_str(), strerror(errno));
            } else if (printPageNum) {
                fprintf(OutputFilePtr, "Page number of deleted key= %u\n", pageNum);
            }
            mdbm_unlock_smart(db, &key, MDBM_O_RDWR);
        }

        MdbmData.DropHandle(db);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmDeleteCommand)

///  mdump (mdbm_dump) - dump contents of an MDBM

// mdump parser class - support the various options of (new_)mdbm_dump using the -x arg:
//  print cache-info (e.g. cache timestamps for cache MDBMs)
//  print information about the chunks only
//  print info on deleted entries
//  print info about free chunks
//  print using legacy format (mdbm_dump_page/dump_all_pages)
//  print info about page headers only
class MdbmDumpParser : public OptionParser
{
public:
    MdbmDumpParser()
    {
        AddLockModeOption();
        RegisterOption(string("w"), OPTION_TYPE_SIZE, WindowSize,
                       string("Window size for opening MDBM in windowed mode"));
        RegisterOption(string("k"), OPTION_TYPE_FLAG, PrintKeys, string("Print key data"));
        RegisterOption(string("V"), OPTION_TYPE_FLAG, PrintValues, string("Print value data"));
        RegisterOption(string("P"), OPTION_TYPE_SIGNED, PageNumber,
                       string("Check specified page (-1 means check all pages)"));
        RegisterOption(string("x"), OPTION_TYPE_STRING, ExtraData,
                       string("Extra Dump Flags:\n"
"                cache     - Show cache info\n"
"                chunks    - Show chunks\n"
"                deleted   - Include deleted entries\n"
"                free      - Show free chunks and dump data\n"
"                legacy    - Dump using legacy format\n"
"                pagehdr  -  Dump page headers only"));
        set<string> extraFlags;
        extraFlags.insert("cache");
        extraFlags.insert("chunks");
        extraFlags.insert("deleted");
        extraFlags.insert("free");
        extraFlags.insert("legacy");
        extraFlags.insert("pagehdr");
        SetAllowedValues(ExtraData, extraFlags);
    }
};

// mdump implementation
class MdbmDumpCommand : public MdbmCommand
{
public:
    MdbmDumpCommand() :
        MdbmCommand("mdump", "Dump contents of an MDBM")
    {
        parser = new MdbmDumpParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] FileToDump.mdbm\n");
    }

    // Define bit-flags for passing flags to callback
    static const int DUMP_FLAG_CACHEINFO = 0x1;    // Print cache info
    static const int DUMP_FLAG_DELETED   = 0x2;    // Print deleted entry info
    static const int DUMP_FLAG_KEYS      = 0x4;    // Print keys
    static const int DUMP_FLAG_VALUES    = 0x8;    // Print values

    // chunk-iterator callback function
    static int chunkIteratorFunc(void *user, const mdbm_chunk_info_t *info)
    {
        if (info->page_type == MDBM_PTYPE_FREE) {
            return 0;
        }
        const char* PTYPE[] = {"free", "data", "dir ", "lob "};
        fprintf(OutputFilePtr, "%06d %s %6dp (%9db) prev=%-6d prevnum=%06d",
            info->page_num,
            PTYPE[info->page_type],
            info->num_pages,
            info->num_pages * info->page_size,
            info->prev_num_pages,
            info->page_num-info->prev_num_pages);
        if (info->page_type == MDBM_PTYPE_DATA) {
            fprintf(OutputFilePtr, "    num_entries = %-5d  page_num = %d\n",
                    info->page_data, info->dir_page_num);
        } else if (info->page_type == MDBM_PTYPE_DIR) {
            fputc('\n', OutputFilePtr);
        } else if (info->page_type == MDBM_PTYPE_LOB) {
            fprintf(OutputFilePtr, "    vallen = %-10d  page_num = %d\n",
                   info->page_data, info->dir_page_num);
        }

        return 0;
    }

    // Free chunk iterator callback
    static int freeChunkIterator(void *user, const mdbm_chunk_info_t *info)
    {
        if (info->page_type != MDBM_PTYPE_FREE) {
            return 0;
        }
        fprintf(OutputFilePtr, "%06d free %6dp (%9db) prev=%-6d prevnum=%06d +--next_free   = %d\n",
            info->page_num,
            info->num_pages,
            info->num_pages * info->page_size,
            info->prev_num_pages,
            info->page_num-info->prev_num_pages,
            info->page_data);
        return 0;
    }

    // Dump header information callback
    static int dumpHeadersFunc(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
    {
        if (info->i_entry.entry_index != 0) {
            return 0;
        }
        fprintf(OutputFilePtr, "P%06d/%06d # "
               "entries=%-5d (%6d bytes)  deleted=%-5d (%6d bytes)"
               "  free=%-6d (%6d total)"
               "  sizes=%d/%d/%d\n",
               info->i_page.page_num,
               info->i_page.mapped_page_num,
               info->i_page.page_active_entries,
               info->i_page.page_active_space,
               info->i_page.page_deleted_entries,
               info->i_page.page_deleted_space,
               info->i_page.page_free_space,
               info->i_page.page_free_space + info->i_page.page_deleted_space,
               info->i_page.page_min_entry_size,
               info->i_page.page_max_entry_size,
               info->i_page.page_active_entries
               ? (info->i_page.page_active_space / info->i_page.page_active_entries)
               : 0);
        return 0;
    }

    // Dump all data callback
    static int dumpDataFunc(void* user, const mdbm_iterate_info_t* info, const kvpair* kv)
    {
        int dumpFlags = *((int *) user);
        dumpHeadersFunc(user, info, kv);   // Dumping the data also means dumping the page headers

        if ((info->i_entry.entry_flags & MDBM_ENTRY_DELETED) && !(dumpFlags & DUMP_FLAG_DELETED)) {
            return 0;
        }

        fprintf(OutputFilePtr, "  P%06d/%06d [E%04d] %s klen=%-5d vlen=%-5d koff=%-5d voff=%-5d",
            info->i_page.page_num,
            info->i_page.mapped_page_num,
            info->i_entry.entry_index,
            (info->i_entry.entry_flags & MDBM_ENTRY_DELETED)
            ? "D"
            : ((info->i_entry.entry_flags & MDBM_ENTRY_LARGE_OBJECT)
               ? "L"
               : " "),
            kv->key.dsize,
            kv->val.dsize,
            info->i_entry.key_offset,
            info->i_entry.val_offset);

        if (info->i_entry.entry_flags & MDBM_ENTRY_LARGE_OBJECT) {
            fprintf(OutputFilePtr, " lob-page=%d/%p lob-pages=%d",
                   info->i_entry.lob_page_num,
                   (void*)info->i_entry.lob_page_addr,
                   info->i_entry.lob_num_pages);
        }
        if (dumpFlags & DUMP_FLAG_CACHEINFO) {
            fprintf(OutputFilePtr, "    num_accesses=%u access_time=%u (t-%lu) priority=%f",
                   info->i_entry.cache_num_accesses,
                   info->i_entry.cache_access_time,
                   info->i_entry.cache_access_time
                   ? (long)(get_time_sec() - info->i_entry.cache_access_time)
                   : 0,
                   info->i_entry.cache_priority);
        }
        fputc('\n', OutputFilePtr);
        if (dumpFlags & DUMP_FLAG_KEYS) {
            PrintDatum(OutputFilePtr, "  K ", kv->key);
        }
        if (dumpFlags & DUMP_FLAG_VALUES) {
            PrintDatum(OutputFilePtr, "  V ", kv->val);
        }
        return 0;
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        // Allow one arg: the filename
        if ((ret = ParseVerify(args, usageMsg, stopNow, 1)) || stopNow) {
            return ret;
        }

        string fullPath;
        MDBM *db = OpenExisting(0, parser->optionsData.extra[0], fullPath, true);
        if (db == NULL) {
            return -1;
        }
        int flags = 0;
        bool noLock = (mdbm_get_lockmode(db) == MDBM_OPEN_NOLOCK);
        if (noLock) {
            flags |= MDBM_ITERATE_NOLOCK;
        }

        // Used to pass flags to mdbm_iterate for print key, print value, -x "cache" and -x "deleted"
        int dumpFlags = 0;
        bool printKey = false;
        parser->optionsData.GetOption(PrintKeys, printKey);
        if (printKey) {
            dumpFlags |= DUMP_FLAG_KEYS;
        }
        bool printVal = false;
        parser->optionsData.GetOption(PrintValues, printVal);
        if (printVal) {
            dumpFlags |= DUMP_FLAG_VALUES;
        }
        int64_t pageNum = -1;
        parser->optionsData.GetOption(PageNumber, pageNum);

        bool chunks = false, freeChunks = false, legacy = false, pageHdrs = false;
        string extraFlags;
        parser->optionsData.GetOption(ExtraData, extraFlags);
        if (strcasecmp(extraFlags.c_str(), "cache") == 0) {
            dumpFlags |= DUMP_FLAG_CACHEINFO;
        }
        if (strcasecmp(extraFlags.c_str(), "chunks") == 0) {
            chunks = true;
        }
        if (strcasecmp(extraFlags.c_str(), "deleted") == 0) {
            dumpFlags |= DUMP_FLAG_DELETED;
        }
        if (strcasecmp(extraFlags.c_str(), "free") == 0) {
            freeChunks = true;
        }
        if (strcasecmp(extraFlags.c_str(), "legacy") == 0) {
            legacy = true;
        }
        if (strcasecmp(extraFlags.c_str(), "pagehdr") == 0) {
            pageHdrs = true;
        }

        if (legacy) {
            if (pageNum < 0) {
                mdbm_dump_all_page(db);
            } else {
                mdbm_dump_page(db, pageNum);
            }
            return 0;
        }
        if (freeChunks) {
            // Iterate through free chunks with chunk_iterate
            mdbm_chunk_iterate(db, freeChunkIterator, flags, NULL);
            // Do not return - emulate (probably incorrect) behavior of mdbm_dump -f
            // which actually (probably incorrectly) prints dump of mdbm data
        }
        if (chunks) {
            // Iterate through chunks with chunk_iterate
            mdbm_chunk_iterate(db, chunkIteratorFunc, flags, NULL);
            return 0;
        }
        if (pageHdrs) {
            mdbm_iterate(db, (int) pageNum, dumpHeadersFunc, flags, NULL);
            return 0;
        }
        // If not requested page headers only, then iterate through page-entries
        flags |= MDBM_ITERATE_ENTRIES;
        mdbm_iterate(db, (int) pageNum, dumpDataFunc, flags, &dumpFlags);
        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmDumpCommand)

///  mtrunc - truncate an existing MDBM

// mtrunc parser class
class MdbmTruncParser : public MdbmOptionParser
{
public:
    MdbmTruncParser()
    {
        DeleteOption(PageSize);   // Page size cannot change when truncating
        RegisterOption(string("f"), OPTION_TYPE_FLAG, Force,
                       string("Force truncation (disable confirmation)"));
    }
};

// mtrunc implementation
class MdbmTruncCommand : public MdbmCommand
{
public:
    MdbmTruncCommand() :
        MdbmCommand("mtrunc", "Truncate an MDBM")
    {
        parser = new MdbmTruncParser();
    }

    virtual string GetCommandFormat()
    {
        return cmdName + string(" [OptionsBelow] FileToTruncate.mdbm\n");
    }

    virtual int Run(const vector<string> &args)
    {
        string usageMsg;
        int ret = 0, stopNow = 0;
        if ((ret = ParseVerify(args, usageMsg, stopNow, 1)) || stopNow) {   // One arg only
            return ret;
        }

        bool force = false;
        parser->optionsData.GetOption(Force, force);
        if (!force) {
            fprintf(stdout,
                "WARNING:  This will remove configuration and ALL DATA from the specified MDBM:\n");
            fprintf(stdout, "Are you sure you want to do this (y/n): ");
            fflush(stdout);

            int c = getchar();
            if ('y' != tolower(c)) {
                fprintf(stdout, "Skipping.\n");
                return 0;
            }
        }

        int flags = MDBM_O_CREAT | MDBM_O_TRUNC;
        string fullPath;
        MDBM *db = OpenExisting(flags, parser->optionsData.extra[0], fullPath, false);
        if (db == NULL) {
            return -1;
        }
        uint64_t pageSize = mdbm_get_page_size(db);   // Page size cannot change when truncating
        fprintf(OutputFilePtr, "Truncating %s.\n", fullPath.c_str());
        mdbm_truncate(db);
        MdbmData.DropName(fullPath, true);   // Need to completely close after truncation

        uint64_t preSize, mainDBSize, spillSize;
        int cacheModeParam;
        flags |= MDBM_O_RDWR;

        // Reopen with all options that you can specify in mcreate
        if (!parser->GetCreateOptions(flags, cacheModeParam, preSize,
                                      pageSize, mainDBSize, spillSize, MDBM_ANY_LOCKS)) {
            fprintf(ErrorFilePtr, "Unable to handle options, %s", usageMsg.c_str());
            return -1;
        }

        db = OpenDBWithParams(fullPath.c_str(), flags, 0666, cacheModeParam,
                              preSize, pageSize, mainDBSize, spillSize);
        MdbmData.DropHandle(db);
        mdbm_close(db);

        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmTruncCommand)

///  mdigest - displays a digest/hash-code of an MDBM

// mdigest parser class
class MdbmDigestParser : public OptionParser
{
public:
    MdbmDigestParser()
    {
         RegisterOption(string("H"), OPTION_TYPE_UNSIGNED, HashFunc,
                       string("Hash function for producing a digest\n"
"                   0 SHA-1 (default)\n"
"                   1 MD5\n"));
        SetMaxAllowed(HashFunc, 1);

        RegisterOption(string("v"), OPTION_TYPE_FLAG, VerbosityLevel, string("Verbose output"));
    }
};


// mdigest implementation
class MdbmDigestCommand : public MdbmSimpleCommand
{
public:
    MdbmDigestCommand() :
        MdbmSimpleCommand("mdigest", "Display a digest/hash-code of an MDBM")
    {
        parser = new MdbmDigestParser();
    }

    virtual int Run(const vector<string> &args)
    {
        return SimpleRun(args, string("hashing"));
    }

protected:
    int workFunc(const char *filename)
    {
        bool verbose = false;
        parser->optionsData.GetOption(VerbosityLevel, verbose);
        uint64_t hash = 0;   // 0=SHA-1, 1=MD5
        parser->optionsData.GetOption(HashFunc, hash);

        string fullPath;
        MDBM *db = OpenExisting(0, filename, fullPath);
        if (db == NULL) {
            return -1;
        }

        if (mdbm_lock_smart(db, NULL, MDBM_O_RDONLY) != 1) {
            fprintf(ErrorFilePtr, "%s: Unable to lock MDBM: %s\n",
                    fullPath.c_str(), strerror(errno));
            MdbmData.DropHandle(db);
            return -1;
        }

        kvpair kv;
        MD5_CTX md5_ctx;
        SHA_CTX sha_ctx;
        uint64_t nitems = 0, keybytes = 0, valbytes = 0;

        MD5_Init(&md5_ctx);
        SHA1_Init(&sha_ctx);

        kv = mdbm_first(db);
        while (kv.val.dptr) {
            nitems++;
            keybytes += kv.key.dsize;
            valbytes += kv.val.dsize;
            if (hash) {  // MD5
                MD5_Update(&md5_ctx,kv.key.dptr,kv.key.dsize);
                MD5_Update(&md5_ctx,kv.val.dptr,kv.val.dsize);
            } else {    // SHA-1
                SHA1_Update(&sha_ctx,kv.key.dptr,kv.key.dsize);
                SHA1_Update(&sha_ctx,kv.val.dptr,kv.val.dsize);
            }
            kv = mdbm_next(db);
        }

        if (hash) {    // MD5
            unsigned char d[MD5_DIGEST_LENGTH];

            MD5_Final(d,&md5_ctx);
            fprintf(OutputFilePtr, "MD5(%s)= ", fullPath.c_str());
            for (uint32_t i = 0; i < sizeof(d); ++i) {
                fprintf(OutputFilePtr, "%02x",d[i]);
            }
            fputc('\n', OutputFilePtr);
        } else {       // SHA-1
            unsigned char d[SHA_DIGEST_LENGTH];

            SHA1_Final(d,&sha_ctx);
            fprintf(OutputFilePtr, "SHA1(%s)= ", fullPath.c_str());
            for (uint32_t i = 0; i < sizeof(d); ++i) {
                fprintf(OutputFilePtr, "%02x",d[i]);
            }
            fputc('\n', OutputFilePtr);
        }
        mdbm_unlock_smart(db, NULL, MDBM_O_RDONLY);
        MdbmData.DropHandle(db);

        if (verbose) {
            fprintf(OutputFilePtr, "Total items= %llu\n", (unsigned long long) nitems);
            fprintf(OutputFilePtr, "Total key bytes= %llu\n", (unsigned long long) keybytes);
            fprintf(OutputFilePtr, "Total value bytes= %llu\n", (unsigned long long) valbytes);
        }

        return 0;
    }
};

REGISTER_MASH_COMMAND(MdbmDigestCommand)

