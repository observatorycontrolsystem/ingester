FROM python:3.5
ENV PYTHONBUFFERED 1
ENV APPLICATION_ROOT /ingester

RUN mkdir -p $APPLICATION_ROOT
ADD . $APPLICATION_ROOT
WORKDIR $APPLICATION_ROOT

RUN pip install -r requirements.txt --trusted-host=buildsba.lco.gtn

cmd ["python", "ingester.py"]
