FROM jupyter/pyspark-notebook:spark-3.4.1

USER root

ARG PYTHON_VERSION=3.11.6

RUN conda install --quiet --yes --no-pin \
    python=${PYTHON_VERSION} && \
    conda clean --all -f -y && \
    fix-permissions $CONDA_DIR && \
    fix-permissions /home/$NB_USER

USER ${NB_UID}

CMD ["/usr/local/bin/start.sh", "jupyter", "lab"]