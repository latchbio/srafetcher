FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

WORKDIR /root
RUN apt-get install -y curl unzip wget

# conda envs
ENV CONDA_PREFIX="/root/miniconda3/envs/sra"
ENV CONDA_DEFAULT_ENV="sra"
ENV PATH="/root/miniconda3/envs/sra/bin:/root/miniconda3/bin:${PATH}"

# miniconda3 instllation
RUN apt-get update && apt install --upgrade \
    && apt install aria2 cmake wget curl libfontconfig1-dev git -y\
    && curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh \
    && conda init bash

# activate for following run commands
RUN conda create --name sra
SHELL ["conda", "run", "-n", "sra", "/bin/bash", "-c"]

RUN conda install -c "bioconda/label/main" sra-tools

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
RUN pip install requests
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
CMD ["sleep", "100000"]
