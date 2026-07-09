# Recommendation: Use Debian 12.
FROM debian:12-slim

# Suppress interactive prompts.
ENV DEBIAN_FRONTEND=noninteractive

# Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini libjemalloc2

# Enable jemalloc as default memory allocator
ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2

# Install utilities required by XGBoost for Spark.
RUN apt install -y procps libgomp1

# Install and configure Miniconda3.
ENV CONDA_HOME=/opt/miniforge3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
ADD https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh .
RUN bash Miniforge3-Linux-x86_64.sh -b -p /opt/miniforge3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict
# Packages ipython and ipykernel are required if using custom conda and want to
# use this container for running notebooks.
RUN ${CONDA_HOME}/bin/mamba install ipython ipykernel

#Install Google Cloud SDK.
RUN ${CONDA_HOME}/bin/mamba install -n base google-cloud-sdk

# Install Conda packages.
#
# The following packages are installed in the default image.
# Recommendation: include all packages.
#
# Use mamba to quickly install packages.
RUN ${CONDA_HOME}/bin/mamba install -n base \
    accelerate \
    bigframes \
    cython \
    deepspeed \
    evaluate \
    fastavro \
    fastparquet \
    gcsfs \
    google-cloud-aiplatform \
    google-cloud-bigquery-storage \
    google-cloud-bigquery[pandas] \
    google-cloud-bigtable \
    google-cloud-container \
    google-cloud-datacatalog \
    google-cloud-dataproc \
    google-cloud-datastore \
    google-cloud-language \
    google-cloud-logging \
    google-cloud-monitoring \
    google-cloud-pubsub \
    google-cloud-redis \
    google-cloud-spanner \
    google-cloud-speech \
    google-cloud-storage \
    google-cloud-texttospeech \
    google-cloud-translate \
    google-cloud-vision \
    langchain \
    lightgbm \
    koalas \
    matplotlib \
    mlflow \
    nltk \
    numba \
    numpy \
    openblas \
    orc \
    pandas \
    pyarrow \
    pynvml \
    pysal \
    pytables \
    python \
    pytorch-cpu \
    regex \
    requests \
    rtree \
    scikit-image \
    scikit-learn \
    scipy \
    seaborn \
    sentence-transformers \
    sqlalchemy \
    sympy \
    tokenizers \
    transformers \
    virtualenv \
    xgboost

COPY . .

# Install pip packages.
RUN ${PYSPARK_PYTHON} -m pip install -r requirements.txt

# Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark