FROM python:3.6-alpine

ARG APP_ENV=base

ENV APP_ENV=$APP_ENV
ENV PYTHONUNBUFFERED=1

# Requirements have to be pulled and installed here, otherwise caching won't work
COPY ./requirements /requirements
RUN echo "http://dl-cdn.alpinelinux.org/alpine/edge/community" >> /etc/apk/repositories \
    && apk add --no-cache --virtual .build-deps gcc g++ make git \
    && pip install -r /requirements/${APP_ENV}.txt \
    && rm -rf /requirements \
    && apk del .build-deps

ENV PYTHONPATH=/opt/server
WORKDIR /opt