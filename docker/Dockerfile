FROM ubuntu:16.04

ARG APP_ENV=base

ENV APP_ENV=$APP_ENV
ENV PYTHONUNBUFFERED=1


RUN apt-get update
RUN apt-get install -y software-properties-common vim
RUN add-apt-repository ppa:jonathonf/python-3.6
RUN apt-get update

COPY ./requirements /requirements
COPY ./r-requirements /r-requirements

RUN apt-get -y install build-essential libxml2-dev zlib1g-dev python-dev pkg-config libffi6 libffi-dev libcairo-dev libssl-dev libcurl4-openssl-dev \
                       python3.6-dev python3-pip ipython3 \
                       r-base gfortran \
                       libc6-i386 lib32stdc++6


RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel
RUN pip install -r /requirements/${APP_ENV}.txt 
# RUN apt-get -y install libmariadb-client-lgpl-dev libssl-dev libcurl4-openssl-dev

# RUN R --vanilla -q -f /r-requirements/${APP_ENV}.R

CMD ["bash"]


ENV PYTHONPATH=/opt/server
WORKDIR /opt

