#!/bin/bash
OUTPUT_DIR=$1
RESTORE_VERSION=$2
CASSANDRA_HOME=/home/int/apache-cassandra-2.1.13/

function restore {
	filename=$OUTPUT_DIR/$1-$RESTORE_VERSION.csv
	command="COPY $1 FROM $filename;"
	echo $command
	$CASSANDRA_HOME/bin/cqlsh -e "$command"
}

echo "restore starting..."
restore effechecka.monitors
restore effechecka.selector
restore effechecka.checklist_registry
restore effechecka.occurrence_collection_registry
restore effechecka.subscriptions
echo "restore complete."
