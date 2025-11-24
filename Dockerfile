# Python 3.12
FROM python:3.12-slim-trixie

RUN apt-get update \
    # Install aws-lambda-cpp build dependencies
    && apt-get install -y \
      g++ \
      make \
      cmake \
      unzip \
      curl \
    # cleanup package lists, they are not used anymore in this image
    && rm -rf /var/lib/apt/lists/* \
    && apt-cache search linux-headers-generic

ARG FUNCTION_DIR="/function"

# Copy function code
RUN mkdir -p ${FUNCTION_DIR}

# Install uv package manager and move it to /usr/local/bin
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    mv ~/.local/bin/uv /usr/local/bin/uv && \
    chmod +x /usr/local/bin/uv

RUN uv --version

# Update pip
RUN uv pip install --upgrade pip wheel six setuptools --system \
    && uv pip install --upgrade --no-cache-dir --system \
        awslambdaric \
        boto3 \
        redis \
        httplib2 \
        requests \
        numpy \
        scipy \
        pandas \
        pika \
        kafka-python \
        cloudpickle \
        ps-mem \
        tblib \
        psutil

# Set working directory to function root directory
WORKDIR ${FUNCTION_DIR}

# Set environment variables for Lambda
ENV PYTHONPATH="/var/lang/lib/python3.12/site-packages:${FUNCTION_DIR}"
COPY requirements.txt .
COPY titiler_cmr/ titiler_cmr
RUN uv pip install -e ./titiler_cmr --system
RUN uv pip install -r requirements.txt --system

# Add Lithops
COPY lithops_lambda.zip ${FUNCTION_DIR}
RUN unzip lithops_lambda.zip \
    && rm lithops_lambda.zip \
    && mkdir handler \
    && touch handler/__init__.py \
    && mv entry_point.py handler/

ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "handler.entry_point.lambda_handler" ]
