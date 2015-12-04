FROM python:3.5
ENV VERSION 1
ENV PYTHONBUFFERED 1
ENV APPLICATION_ROOT /ingester

RUN apt-get update
RUN apt-get install -y supervisor

RUN mkdir -p $APPLICATION_ROOT
ADD . $APPLICATION_ROOT
WORKDIR $APPLICATION_ROOT

RUN cp deploy/supervisor-app.conf /etc/supervisor/conf.d/

RUN pip install -r requirements.txt --trusted-host=buildsba.lco.gtn

ENV C_FORCE_ROOT true

EXPOSE 5555

CMD ["supervisord", "-n"]
