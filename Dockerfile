FROM python:3.5-slim
ENV VERSION 1
ENV PYTHONBUFFERED 1
ENV APPLICATION_ROOT /ingester

RUN apt-get update \
        && apt-get install -y supervisor \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

COPY requirements.txt $APPLICATION_ROOT/
RUN pip install -r $APPLICATION_ROOT/requirements.txt --trusted-host=buildsba.lco.gtn \
    && rm -rf ~/.cache/pip ~/.pip

COPY deploy/supervisor-app.conf /etc/supervisor/conf.d/

RUN mkdir -p $APPLICATION_ROOT
ADD . $APPLICATION_ROOT

ENV C_FORCE_ROOT true

EXPOSE 5555

CMD ["supervisord", "-n"]
