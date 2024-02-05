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
- Debezium
- MageAI (temporally in dc-bkp.yml)
- Rapids (NVidea GPU for machine learning)
- Airflow (temporally in dc-bkp.yml)

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

# How to begin the environment

Most of the containers will do it fine on the first run, but you'll have to do this following configurations manually (in the first run only):

#### Create a service user in the Minio S3

After the Minio container is up, you will be able to access the UI by opening http://localhost:9001 it in the web browser. Use the credentials setted in the docker-compose.yml file to access the manegement panel:

```
MINIO_ROOT_USER: minioadmin
MINIO_ROOT_PASSWORD: minioadmin
```

After that, in the Adminstrator section, go to Users in the Identity tab. Create a new user as follows:

```
User Name: minio
Password: minioadmin

Assign Policies:
[x] consoleAdmin
[x] diagnostics
[x] readonly
[x] readwrite
```

Once the user was created, you will be able to access the Minio S3 with the credentials.

#### Download aws-java-sdk-bundle-1.12.588.jar

As this jar is way too large to be pushed here, you will need to download it from the Maven repository in this link:

https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.588/aws-java-sdk-bundle-1.12.588.jar

And save it in the following paths:

```
data/hive-metastore/lib/
data/spark/jars/
```

Remember that you cant commit large files into GitHub, so this JAR is ignored by the gitignore file.

# Any doubts?

Contact me: https://www.linkedin.com/in/luiz-henrique-mm/