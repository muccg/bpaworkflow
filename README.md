# Bioplatforms Australia - Workflow System

Bioplatforms-Workflow is a web application which facilitates data ingests into the Bioplatforms Australia Data Portal.

# Development
Ensure a late version of both docker and docker-compose are available in your environment.

bpaworkflow is available as a fully contained Dockerized stack. The dockerised stack are used for both production
and development. Appropiate configuration files are available depending on usage.

Note that for data ingestion to work you need passwords to the hosted data, these are available from BPA on request.
Set passwords in your environment, these will be passed to the container.

## Quick Setup

* [Install docker and compose](https://docs.docker.com/compose/install/)
* git clone https://github.com/bioplatformsaustralia/bpaworkflow.git
* `./develop.sh build base`
* `./develop.sh build builder`
* `./develop.sh build dev`

`develop.sh up` will spin up the stack. See `./develop.sh` for some utility methods, which typically are simple 
wrappers arround docker and docker-compose.

docker-compose will fire up the stack like below:
```
docker ps -f name="bpaworkflow*"

IMAGE                       PORTS                                                                          NAMES
bioplatformsaustralia/nginx-uwsgi:1.10      0.0.0.0:8080->80/tcp, 0.0.0.0:8443->443/tcp                                    bpaworkflow_nginx_1
mdillon/postgis:9.5         0.0.0.0:32944->5432/tcp                                                        bpaworkflow_db_1
bioplatformsaustralia/bpaworkflow-dev            0.0.0.0:9000-9001->9000-9001/tcp, 8000/tcp, 0.0.0.0:9100-9101->9100-9101/tcp   bpaworkflow_uwsgi_1
bioplatformsaustralia/bpaworkflow-dev            9000-9001/tcp, 0.0.0.0:8000->8000/tcp, 9100-9101/tcp                           bpaworkflow_runserver_1
```