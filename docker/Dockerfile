FROM ubuntu:18.04

ARG APP_ENV=base

ENV APP_ENV=$APP_ENV
ENV PYTHONUNBUFFERED=1
ENV DEBIAN_FRONTEND=noninteractive

# RUN apt-get update \
#     && apt-get install -y software-properties-common \
#     && add-apt-repository ppa:jonathonf/python-3.6

COPY ./requirements /requirements
COPY ./r-requirements /r-requirements

RUN apt-get update && apt-get -y install tzdata
RUN ln -fs /usr/share/zoneinfo/Europe/Madrid /etc/localtime
RUN dpkg-reconfigure --frontend noninteractive tzdata

RUN apt-get update \
    && apt-get -y install build-essential libxml2-dev zlib1g-dev python-dev pkg-config libffi6 libffi-dev libssl-dev libcurl4-openssl-dev \
                          python3.6-dev python3-pip python3-distutils \
                          r-base gfortran \
                          libc6-i386 lib32stdc++6

RUN python3.6 -m pip install pip --upgrade
RUN python3.6 -m pip install wheel
RUN pip install -r /requirements/${APP_ENV}.txt
# RUN apt-get -y install libmariadb-client-lgpl-dev libssl-dev libcurl4-openssl-dev

RUN Rscript --vanilla /r-requirements/${APP_ENV}.R

CMD ["bash"]


ENV PYTHONPATH=/opt/server
WORKDIR /opt

