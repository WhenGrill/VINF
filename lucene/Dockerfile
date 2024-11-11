FROM coady/pylucene

WORKDIR /usr/src/app

RUN pip install --no-cache-dir --upgrade pip

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "lucene_indexer_searcher.py"]