FROM debian:buster-slim
WORKDIR /usr/local/bin
COPY ./target/release/procurement_microservice /usr/local/bin/procurement_microservice
RUN apt-get update && apt-get install -y
RUN apt-get install curl -y
STOPSIGNAL SIGINT
ENTRYPOINT ["procurement_microservice"]