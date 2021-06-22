use master
go
create login msync with password='?'
go
use db
go
create user msync from login msync
go
grant update, insert, select, delete, execute on schema::rpl to msync

