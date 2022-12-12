FROM harbor.kube.itdw.io/docker/library/python:3.11-slim-bullseye
ENV PYTHONUNBUFFERED=1
LABEL org.opencontainers.image.authors="support@it-dw.com"
WORKDIR /code
COPY requirements.txt /code/requirements.txt
COPY ./ /code/
RUN pip install --upgrade pip && pip install --no-cache-dir -r /code/requirements.txt
CMD ["python", "main.py"]
