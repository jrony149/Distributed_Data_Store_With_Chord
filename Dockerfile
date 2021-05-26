FROM python:3
ADD src src
COPY . /

COPY requirements.txt .
RUN pip3 install -r requirements.txt  

CMD [ "python3", "./src/app.py" ]
