# Use Miniconda as the base image to manage Python environment and dependencies
FROM continuumio/miniconda3:23.10.0-1 AS build

# Install necessary packages and conda-pack for environment packaging
RUN conda config --set always_yes yes --set changeps1 no && \
    conda update --all -y && \
    conda config --add channels conda-forge && \
    conda install -c conda-forge conda-pack

# Install libmamba and set it as default solver for faster dependency resolution
RUN conda install -n base conda-libmamba-solver && \
    conda config --set solver libmamba

# Copy your environment.yml into the Docker image and create a conda environment based on it
COPY environment.yaml .
RUN conda env create -f environment.yaml && \
    conda install -c conda-forge conda-pack && \
    conda-pack -n $(head -1 environment.yaml | cut -f 2 -d ":" | sed -e 's/^[[:space:]]*//' -) -o /tmp/env.tar && \
    conda clean --all --force-pkgs-dirs -y && \
    mkdir /env && cd /env && tar xf /tmp/env.tar && \
    rm /tmp/env.tar

# Unpack the environment to ensure scripts are properly set up with correct paths
RUN /env/bin/conda-unpack

# Use Debian slim as the runtime base image
FROM debian:buster-slim AS runtime

# Copy the packed environment from the build stage
COPY --from=build /env /env

# Install bash, git, wget, and curl
RUN apt-get update && \
    apt-get -y install bash git wget curl && \
    apt-get clean all && \
    apt-get purge && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# Copy your app files and scripts into the Docker image
COPY app.py /app/
COPY utils.py /app/
COPY users.py /app/
COPY crud.py /app/
COPY schema.py /app/
COPY classification.py /app/
COPY feature.py /app/
COPY labels.py /app/
COPY run_server.sh /app/
COPY setup_run_server.sh /app/

# Set the working directory to /app
WORKDIR /app

# Open port 8000 to traffic
EXPOSE 8000

# Ensure the environment is activated when the container starts and run your setup script
ENTRYPOINT ["/bin/bash", "-c", "source /env/bin/activate && exec python -m panel serve app.py --address 0.0.0.0 --port 8000 --allow-websocket-origin='*'"]

