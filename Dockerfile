FROM archlinux/base

RUN pacman -Sy --noconfirm gcc git go make

ENV GOPATH=/go/.build
WORKDIR /go
