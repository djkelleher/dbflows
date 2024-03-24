FROM mambaorg/micromamba:jammy

# mambaorg/micromamba defaults to a non-root user.
USER root

ENV PROJ_DIR="/opt/dbflows"

RUN apt-get update && DEBIAN_FRONTEND=noninteractive \
    apt-get install -y --no-install-recommends \
    lsb-release libpq-dev gcc wget ca-certificates \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc | tee /etc/apt/trusted.gpg.d/pgdg.asc &>/dev/null \
    && apt-get update

RUN DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends postgresql-client-15

# activate base environment.
ARG MAMBA_DOCKERFILE_ACTIVATE=1

# Install Python
RUN micromamba install --yes --name base -c conda-forge python=3.11
RUN micromamba clean --all --yes

# copy files.
WORKDIR $PROJ_DIR
COPY . .

RUN pip install -e .[dev]