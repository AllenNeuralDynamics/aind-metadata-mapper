FROM python:3.10-bullseye
WORKDIR /app
ADD src ./src
ADD pyproject.toml .
ADD setup.py .

RUN pip install --upgrade pip
RUN pip install .[all] --no-cache-dir
