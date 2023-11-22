# spark-lab

A simple environment for data processing in home or study projects.

# Model

![Model](https://github.com/luizhenriquemm/spark-lab/blob/main/model.png)

# List of applications
- Spark
- Jupyter with PySpark
- PostgreSQL
- Hive Metastore
- Minio
- Kafka
- NiFi 
- Zookeeper
- TrinoDB
- Metabase

# Custom images

The Jupyter image is the only that needs a custom build. It's depends on a custom python version, the respective Dockerfile is present in the images folder.

Remember that you don't need to build it, in the first docker compose up command, the composer will build it automatically. But, if you need to force the build for some reason, you can use this command:

```sh
docker build images/jupyter/. -t 3.4.1
```

For all other applications, only the docker-compose.yml is needed.

# Persistent data and volumes path

As every container users a mounted volume for data persistence, there's a .env file in this repository that sets the BASE_PATH variable. You'll need to change that for the cloned repository path into your computer.

#### Consider the gitignore

Most of the saved data can be too large for beeing pushed into GitHub. Remember that if you need to move your environment to somewhere else.

# Any doubts?

Contact me: https://www.linkedin.com/in/luiz-henrique-mm/