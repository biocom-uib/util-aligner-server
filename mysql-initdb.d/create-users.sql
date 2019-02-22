create user if not exists 'util-aligner-admin'@'127.0.0.1' identified by 'util-aligner-admin';
grant all on protein_db.* to 'util-aligner-admin'@'127.0.0.1';

create user if not exists 'util-aligner-updater'@'172.20.0.1' identified by 'util-aligner-updater';
grant all on protein_db.* to 'util-aligner-updater'@'172.20.0.1';

create user if not exists 'util-aligner-server'@'172.20.0.2/255.255.255.254' identified by 'util-aligner-server';
grant all on protein_db.* to 'util-aligner-server'@'172.20.0.2/255.255.255.254';

create user if not exists 'util-aligner-api'@'%' identified by 'util-aligner-api';
grant select on protein_db.* to 'util-aligner-api'@'%';

delete from mysql.user where user='root' and host not in ('localhost', '127.0.0.1', '::1');

flush privileges;
