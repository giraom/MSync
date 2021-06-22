--Run this to build the commands to subscribe the tables you wish
select s.name, o.name
	,'exec rpl.spSubscribeTable 1, '''+s.name+''','''+o.name+''''
	--select *
 from sys.objects o
 join sys.schemas s on o.schema_id = s.schema_id
 where o.type='u'
 --and s.name in ('Person')
 and o.name not in ('import_stats','sysdiagrams')
 order by 1,2

/*Examples:
exec rpl.spCreateSubscription @Name = 'Person', @PriorityGroup = 1, @Server = 'SQLVM1', @Database = 'AdventureWorks2019'

exec rpl.spSubscribeTable 1, 'Person','Address'
*/

select * from rpl.Subscription
select * from rpl.SubscriptionTable
select * from rpl.SubscriptionColumn

