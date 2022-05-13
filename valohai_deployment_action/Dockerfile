# We'll use a slim python image as a base
FROM python:3.8.2-slim-buster

# Our code will need requests, so we can install them on the image with pip
RUN pip install requests

RUN pip install PyYAML==6.0

# Add our files
ADD valohai_deployment.py /valohai_deployment.py

ADD valohai.yaml /valohai.yaml