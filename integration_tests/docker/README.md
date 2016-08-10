# Test services with docker-compose

First, install docker-compose. Then, run a test infrastructure with
```
docker-compose up --build -d
```
Now there are two CouchDB databases running, a task database on port 5784 and a job database on port 5884 (user simcityadmin, password simcity).  A webdav server is running on port 8888 (user webdav, password vadbew).  And finally, a Slurm cluster is accessible over SSH on port 10022 (user xenon, password javagat).  Run simcity commands in this directory, the config is already set up in `config.ini`.

These tests are based on the ones in [Xenon](http://nlesc.github.io/Xenon).
