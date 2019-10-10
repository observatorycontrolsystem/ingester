FROM python:3.5
ENV VERSION 1
ENV PYTHONBUFFERED 1
ENV C_FORCE_ROOT true

EXPOSE 5555

WORKDIR /ingester

CMD ["supervisord", "-n"]

RUN apt-get update \
        && apt-get install -y supervisor \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip install -r requirements.txt --trusted-host=buildsba.lco.gtn \
    && rm -rf ~/.cache/pip ~/.pip

COPY deploy/supervisor-app.conf /etc/supervisor/conf.d/
