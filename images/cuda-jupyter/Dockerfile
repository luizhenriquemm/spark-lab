FROM nvcr.io/nvidia/tensorflow:23.10-tf2-py3

RUN pip install jupyterlab

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--allow-root", "--no-browser"]


