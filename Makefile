DOCKER_IMG := docker.lco.global/ingester

GIT_DIRTY := $(shell git status --porcelain)
GIT_TAG := $(shell git describe --always)

# Add a suffix to the tag if the repo is dirty
ifeq ($(GIT_DIRTY),)
TAG := $(GIT_TAG)
else
TAG := $(GIT_TAG)-dirty
endif

all: docker-build docker-push

docker-build:
	docker build --tag $(DOCKER_IMG):$(TAG) .

docker-push:
	docker push $(DOCKER_IMG):$(TAG)
