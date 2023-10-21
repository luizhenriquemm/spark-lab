# spark-lab

# Model



# Creating the image

The Jupyter image depends on a custom python version, the Dockerfile present here is for that.

Creating the image:

```bash
docker build . -t 3.4.1
```

# Changing the volumes path

In all volumes, you must change the path for the repo clone path.