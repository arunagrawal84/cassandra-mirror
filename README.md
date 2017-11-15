# cassandra-mirror

cassandra-mirror uploads Cassandra SSTable to a blobstore (currently S3 is
supported). During backup, it seeks to be as efficient as possible with data
transfer, only uploading each SSTable once. For restore, it attempts to
operate as quickly as possible by saturating available resources.

# Copyright

This project is available under the Apache v2 license (see LICENSE).  Commits
f66630a62 and prior are copyright 2017 Fitbit Inc., and licensed under Apache
v2.
