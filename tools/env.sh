# Toolchain locations for this machine. Source it, do not execute it:
#
#     source tools/env.sh
#     mvn -o -pl core test
#
# To check what it resolves without sourcing:  sh tools/env.sh --show
#
# The JDK and Maven live under ~/jdk and are NOT on the default PATH, so
# a non-interactive shell (a CI step, an agent session, anything that
# does not load the interactive profile) sees no `java` and no `mvn`.
# `/usr/bin/java` is only macOS's stub, which reports "Unable to locate
# a Java Runtime" and looks exactly like nothing being installed at all.
#
# Adding this file rather than editing tools/allgates.sh is deliberate:
# allgates.sh calls plain `mvn` and keeps working unchanged once this has
# been sourced.
#
# No globs are used below on purpose. An unmatched glob in a `for` list
# is a hard error in zsh, which would make this file fail noisily on a
# machine laid out differently.

_ll_jdk_root="$HOME/jdk"

if [ -d "$_ll_jdk_root" ]; then
    # Newest matching entry wins, so a JDK upgrade needs no edit here.
    for _ll_d in $(ls -1 "$_ll_jdk_root" 2>/dev/null | sort); do
        case "$_ll_d" in
            jdk-*)
                if [ -x "$_ll_jdk_root/$_ll_d/Contents/Home/bin/java" ]; then
                    JAVA_HOME="$_ll_jdk_root/$_ll_d/Contents/Home"
                elif [ -x "$_ll_jdk_root/$_ll_d/bin/java" ]; then
                    JAVA_HOME="$_ll_jdk_root/$_ll_d"
                fi
                ;;
            apache-maven-*)
                if [ -x "$_ll_jdk_root/$_ll_d/bin/mvn" ]; then
                    _ll_mvn_bin="$_ll_jdk_root/$_ll_d/bin"
                fi
                ;;
        esac
    done
fi

if [ -n "${JAVA_HOME:-}" ]; then
    export JAVA_HOME
    PATH="$JAVA_HOME/bin:$PATH"
else
    echo "tools/env.sh: no JDK found under $_ll_jdk_root" >&2
fi

if [ -n "${_ll_mvn_bin:-}" ]; then
    PATH="$_ll_mvn_bin:$PATH"
else
    echo "tools/env.sh: no Maven found under $_ll_jdk_root" >&2
fi

export PATH

# Offline by default: ~/.m2 is fully populated (~2.3G). The gates that
# genuinely need to resolve set MVN_OFFLINE=0 for themselves.
export MVN_OFFLINE="${MVN_OFFLINE:-1}"

unset _ll_jdk_root _ll_d _ll_mvn_bin

if [ "${1:-}" = "--show" ]; then
    echo "JAVA_HOME=${JAVA_HOME:-(none)}"
    echo "java:  $(command -v java || echo none)"
    echo "mvn:   $(command -v mvn || echo none)"
    java -version 2>&1 | head -1
    mvn -version 2>&1 | head -1
fi
