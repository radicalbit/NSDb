#!/bin/bash
mkdir ~/.artifactory/
FILE=$HOME/.artifactory/.credentials
cat <<EOF >$FILE
realm = Artifactory Realm
host = tools.radicalbit.io
user = $REPO_USER
password = $REPO_PASS
EOF
echo "Created ~/.artifactory/.credentials file: Here it is: "
ls -la $FILE