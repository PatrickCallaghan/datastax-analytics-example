create keyspace IF NOT EXISTS datastax_analytics_demo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3' };

use datastax_analytics_demo; 

create table IF NOT EXISTS transactions (
	accid text,
	txtnid uuid,
	txtntime timestamp,
	amount double,
	type text,
	reason text,
	PRIMARY KEY((accid), txtntime)
);

truncate transactions;
