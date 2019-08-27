if ! /usr/libexec/java_home -v 11 &> /dev/null
then
    echo "$0: error: java version 11 does not exist.  Install OpenJDK 11"
    exit 1
fi
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
