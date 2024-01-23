.PHONY: all

all:
	docker build -t ds_server:latest ./server
	docker-compose up