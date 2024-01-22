.PHONY: all

all:
	docker build -t ds_server:latest ./server && python3 load_balancer.py