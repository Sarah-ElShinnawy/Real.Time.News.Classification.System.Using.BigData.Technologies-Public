FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    kafka-python \
    findspark \
    plotly \
    scikit-learn \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    nltk \
    requests \
    spacy

RUN python -m spacy download en_core_web_sm

RUN python -c "import nltk; \
nltk.download('punkt'); \
nltk.download('stopwords'); \
nltk.download('wordnet')"

USER jovyan

ENV SPARK_MASTER=spark://spark-master:7077
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_SUBMIT_ARGS="--master spark://spark-master:7077 pyspark-shell"