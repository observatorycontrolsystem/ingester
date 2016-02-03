FROM python:3.5
ENV VERSION 1
ENV PYTHONBUFFERED 1
ENV APPLICATION_ROOT /ingester

RUN apt-get update
RUN apt-get install -y supervisor

COPY requirements.txt $APPLICATION_ROOT/
RUN pip install -r $APPLICATION_ROOT/requirements.txt --trusted-host=buildsba.lco.gtn

COPY deploy/supervisor-app.conf /etc/supervisor/conf.d/

RUN mkdir -p $APPLICATION_ROOT
ADD . $APPLICATION_ROOT

ENV C_FORCE_ROOT true

EXPOSE 5555

CMD ["supervisord", "-n"]
