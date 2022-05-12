# We'll use a slim python image as a base
FROM python:3.8.2-slim-buster

# Add requirements and install them
ADD requirements.txt /requirements.txt

RUN pip install -r requirements.txt

# Add our files
ADD valohai_deployment.py /valohai_deployment.py

ADD valohai.yaml /valohai.yaml


