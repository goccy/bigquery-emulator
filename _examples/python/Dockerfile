FROM python:3.9.14-bullseye

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install pipenv

WORKDIR /work

COPY Pipfile /work/Pipfile
COPY Pipfile.lock /work/Pipfile.lock

RUN pipenv install

COPY example.py /work/example.py

CMD ["pipenv", "run", "example"]
