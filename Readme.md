Backup Script
=============

Script use to backup files and databases
It manage backup rotations

It's written in perl because it is often already deployed on a lot of servers
It use ssh, rsync and mysqldump, which sould be installed on backuped servers

Some requirement on backup server:

apt-get install libconfig-auto-perl libmime-lite-perl rsync libauthen-sasl-perl libmime-base64-urlsafe-perl



Database user should have the right to read every backuped table
example:
GRANT SELECT, SHOW VIEW, TRIGGER, LOCK TABLES, PROCESS, REPLICATION CLIENT ON *.* TO 'backuper'@'localhost';

