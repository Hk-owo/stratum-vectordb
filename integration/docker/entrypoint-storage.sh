#!/bin/sh
# entrypoint-storage.sh — a storage node's container: the vecstore first, then
# the storage layer itself.
#
# Both run in one container because they share a filesystem. The storage layer
# writes an index's files (.index, .ids) into the node's data directory and the
# vecstore loads them from those same paths, so putting them in one container is
# what lets the vecstore be a plain local process again instead of something the
# host has to reach into the node's data directory to serve.
#
# The vecstore listens on the container's loopback only: it is the storage
# layer's private engine, not an endpoint peers or clients talk to. Peer traffic
# is DataSyncService, which the storage layer serves.
set -e

VECSTORE_DATA="${STRATUM_VECSTORE_DATA:-/var/lib/stratum/vecstore}"
VECSTORE_ADDR="${STRATUM_VECSTORE_ADDR:-127.0.0.1:7100}"

mkdir -p /var/log/stratum

# The control role keeps no data storage, so it has no index for a vecstore to
# serve. Starting one — or even creating its data directory — would leave the
# container with storage-shaped leftovers it has no use for, which is exactly
# what this role is meant not to have.
ROLE="$(sed -n 's/^[[:space:]]*role:[[:space:]]*//p' /etc/stratum/config.yaml | head -n1)"
if [ "$ROLE" = "control" ]; then
  echo "role=control: no vecstore to start, no vecstore data dir to create" >&2
  exec /usr/local/bin/stratum -config /etc/stratum/config.yaml
fi

mkdir -p "$VECSTORE_DATA"

# The host's loader runs the host-linked binary with the host's libraries
# alongside it — see the note in Dockerfile.storage.
/opt/stratum/lib/ld-linux-x86-64.so.2 --library-path /opt/stratum/lib \
  /opt/stratum/vecstore_server \
  --grpc_addr="$VECSTORE_ADDR" \
  --rocksdb_path="$VECSTORE_DATA" \
  >>/var/log/stratum/vecstore.log 2>&1 &

# Let it bind before the storage layer's startup reconcile asks it for indexes.
# A miss here is not fatal (that reconcile degrades to a warning), but starting
# in order keeps the boot log free of an error that is not one.
sleep 2

exec /usr/local/bin/stratum -config /etc/stratum/config.yaml
