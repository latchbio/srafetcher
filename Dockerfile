FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9c8f-main

WORKDIR /root

RUN apt-get update && apt-get install curl pigz -y

# explicitly setting version bc the "current" distribution is out of date
ENV SRA_TOOLKIT_VERSION="3.0.2"

RUN curl -O https://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/$SRA_TOOLKIT_VERSION/sratoolkit.$SRA_TOOLKIT_VERSION-ubuntu64.tar.gz &&\
    tar -xvzf sratoolkit.$SRA_TOOLKIT_VERSION-ubuntu64.tar.gz

ENV PATH=/root/sratoolkit.$SRA_TOOLKIT_VERSION-ubuntu64/bin:$PATH

COPY latch /root/latch
RUN pip install /root/latch
RUN pip install --upgrade pysradb

COPY wf /root/wf

ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
