Backup Script
=============

## Presentation

Really basic script to backup files and databases from distant machines
It manages backup rotations and can store files on AWS Glacier


## Requirements

### TL:DR;

On the backup server:
```
sudo apt-get install scp python3 python3-boto3 python3-ruamel.yaml python3-requests openssh-client rsync pigz
```

On the servers to backup:
```
ssh your.server.tld bash -c 'sudo apt-get install rsync [mongodump] [pg_dump] [mysqldump]'
```

### Details

It requires only python (2.7 or 3+)

Most of the other requirements depends of what you want to backup:
* distant servers: **openssh-client** in this machine, openssh-server on distant machines
* mysql databases: **mysqldump** on the machine running the database
* mongo databases: **mongodump** on the machine running the database
* postgres databases: **pg_dump** on the machine running the database

### Optional dependencies:

* **python-yaml** or **ruamel.yaml**: If you want your configuration to be yaml instead of json
* **pigz**: more efficient than gzip
* **rsync**: for fast file fetching
* **boto3** or **boto** or **awscli**: to upload backups on AWS glacier


## Right managements:

The main idea is that backup configuration file should not contains credentials.
So the credentials should be provided by other means (~/.ssh/id_rsa, ~/.my.cnf, etc...)

Database user should have the right to read every backuped table

* mysql example:
```
GRANT SELECT, SHOW VIEW, TRIGGER, LOCK TABLES, PROCESS, REPLICATION CLIENT ON *.* TO 'backuper'@'localhost';
```

* postgres example:
```
GRANT SELECT ON ALL TABLES IN SCHEMA public TO backuper;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO backuper;
```

