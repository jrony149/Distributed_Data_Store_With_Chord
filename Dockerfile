FROM python:3
#ADD src src
COPY . /

#COPY requirements.txt .
RUN pip install -r requirements.txt  

CMD [ "python3", "app.py" ]
