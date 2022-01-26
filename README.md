# PLAS tesk-core

**PLAS tesk-core** is part of the [PLAS-TESK](https://github.com/PlatformedTasks/PLAS-TESK) and is an extension of the original [tesk-core](https://github.com/elixir-cloud-aai/tesk-core).
It is an element of the [PLAS project](https://github.com/PlatformedTasks/Documentation) funded by the [GÉANT Innovation Programme](https://community.geant.org/community-programme-portfolio/innovation-programme/) initiative to extend the [GÉANT Cloud Flow (GCF)](https://clouds.geant.org/community-cloud/) to be capable of performing platformed-tasks in the cloud.

This repository includes the code of the image of the *taskmaster* that is run as a container into the Kubernetes cluster by the TESK-API. 
It can be pulled from Docker Hub using the standard Docker syntax:

```console
docker pull platformedtasks/plas-taskmaster:latest
```

If you want to build your own version, you can execute:

```console
docker build -t IMAGE_NAME -f "containers/plas-taskmaster.Dockerfile" .
```
