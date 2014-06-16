# Last modified: Wed Sep 28 17:32:33 2011

define wh
where
end

define exit
quit
end

set print demangle on
set print elements 2000
# Set listsize *after* print elements
set listsize 20

#stop sigsegv
handle SIGABRT stop
handle SIGBUS stop
handle SIGKILL stop
handle SIGQUIT stop
handle SIGSEGV stop
handle SIGTERM stop

#set follow-fork-mode parent
set follow-fork-mode child
show follow-fork-mode

# After modules have been loaded, breakpoints may be set in them.
#break main
#run 

# Delete the breakpoint, so a subsequent `run' won't stop at it.
#delete 1

# After started, set scheduler-locking.
#set scheduler-locking on

# info threads -- show threads
# threads apply all backtrace -- backtrace for all threads

# 
# Local variables:
# eval: (if (fboundp 'shell-script-mode) (shell-script-mode))
# eval: (auto-fill-mode 0)
# eval: (if (fboundp 'ts-mode) (ts-mode 1))
# comment-start: "#"
# comment-end: ""
# indent-tabs-mode: t
# End:
