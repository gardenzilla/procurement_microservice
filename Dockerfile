FROM fedora:33
RUN dnf update -y && dnf clean all -y
WORKDIR /usr/local/bin
COPY ./target/release/procurement_microservice /usr/local/bin/procurement_microservice
STOPSIGNAL SIGINT
ENTRYPOINT ["procurement_microservice"]
