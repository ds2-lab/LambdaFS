USERNAME="ben"
SCRIPT="sudo ndbmtd"
HOSTS=$(<$1)
for HOSTNAME in ${HOSTS} ; do
    echo "Launching NDB DataNode on host $HOSTNAME now..."
    ssh -l ${USERNAME} ${HOSTNAME} "${SCRIPT}"
done