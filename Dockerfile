FROM ubuntu:xenial

MAINTAINER GeoComm
RUN echo "deb http://archive.ubuntu.com/ubuntu/ xenial main universe" >> /etc/apt/sources.list && apt-get update

RUN apt-get install -y tar \
                   git \
                   curl \
                   nano \
                   wget \
                   dialog \
                   net-tools \
                   build-essential \
                   checkinstall \
                   libreadline-gplv2-dev \
                   libncursesw5-dev \
                   libssl-dev \
                   libsqlite3-dev \
                   tk-dev \
                   libgdbm-dev \
                   libc6-dev \
                   libbz2-dev \
                   software-properties-common \
                   python-software-properties

RUN wget https://www.python.org/ftp/python/3.6.1/Python-3.6.1.tgz

RUN tar -xvf Python-3.6.1.tgz

WORKDIR /Python-3.6.1
RUN ./configure && make install && python3.6 -m pip install virtualenv

RUN add-apt-repository -y ppa:ubuntugis/ppa && apt-get update
RUN apt-get install -y gdal-bin libgdal-dev
ARG CPLUS_INCLUDE_PATH=/usr/include/gdal
ARG C_INCLUDE_PATH=/usr/include/gdal

WORKDIR /app/sifmonitor
COPY requirements.txt /app/requirements.txt
COPY sifmonitor/logs /app/sifmonitor/logs
COPY /sifmonitor /app/sifmonitor 

#Define environment variable

  
EXPOSE 80
EXPOSE 9000
EXPOSE 9001
EXPOSE 9002

WORKDIR /app
RUN virtualenv --python python3.6 venv && /bin/bash -c "source ./venv/bin/activate" && ./venv/bin/pip install -r requirements.txt

# Set the default directory where CMD will execute
WORKDIR /app/sifmonitor

# Set the default command to execute
# when creating a new container
CMD /bin/bash -c "source ../venv/bin/activate && python check_app_status.py"

