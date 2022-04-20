# MSync
MSync is an open-source tool that allows for full and incremental near real time SQL to SQL synchronization with minimal code. 
It works with SQL on premise, Azure SQL and Managed Instance as both publisher and subscriber. It was built independently and is distributed as-is, with no warranty. Anyone may use, modify and redistribute at will. 
Components:
1. Publisher code. You apply the script in the source database then call proc “rpl.spPublishTable” for each table, which does:
Adds a rowversion column to the table. This column works as a unique auto increment at the database level and gets automatically updated by SQL during inserts and updates. Users cannot tamper with this value. The internal data type is varbinary(8).
 a. Creates an index on the new rv column;
 b. Creates a table to log deleted records with the PK columns;
 c. Creates an after trigger for delete only to log the PK values deleted;
 d. Creates a proc rpl.spGetTableName to be used by the subscribers.
These are the core elements of the change tracking/extraction mechanism. A table must have a PK or unique index for incremental feeds. The PK columns must not be updatable.

2. Subscriber code. You create the tables empty on the subscriber db, apply the subscriber script, create a subscription and add tables to it, which does:
 a. Creates a staging table to bulk insert results from publisher.
 b. Creates a proc to merge data from staging into the subscribed table.

3. The MSync tool. This is a command prompt utility that you point to the subscriber, and it figures out what/how to copy at runtime, all metadata driven. You can schedule this to run every few every few minutes or seconds.  




