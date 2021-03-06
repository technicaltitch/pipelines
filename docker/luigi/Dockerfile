FROM continuumio/miniconda3:latest

RUN apt-get -qq update && \
    apt-get -qq upgrade && \
    apt-get -qq install -y \
        apt-transport-https \
        vim \
        curl \
        postgresql-client && \
        graphviz && \
        libgraphviz-dev && \
        pkg-config && \
    apt-get clean && \
    apt-get -y autoremove && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ARG INCLUDE_LATEX
RUN if [ -n "${INCLUDE_LATEX}" ]; then \
        apt-get -qq update && \
        apt-get -qq install -y \
            make \
            texlive-latex-recommended \
            texlive-fonts-recommended \
            texlive-latex-extra \
            latexmk && \
        apt-get clean && \
        apt-get -y autoremove && \
        rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* ; \
    else \
        echo "INCLUDE_LATEX is not set - skipping LaTeX install" ; \
    fi

WORKDIR /usr/src/app

# Install Python dependencies
COPY requirements requirements
RUN conda config --append channels conda-forge
RUN conda install --quiet --yes --file requirements/base.conda
RUN pip install --quiet --requirement requirements/base.pip

# Set up mostly static layers that don't change often
ENV PYTHONIOENCODING UTF-8
EXPOSE 8082
ENTRYPOINT ["/usr/src/app/docker/luigi/run_luigi.sh"]

# Create the user and required directories
# - docs/_build is a VOLUME in the override-setup, and if doesn't
#   already exist when run_superset.sh runs with docker-compose.override.yml,
#   it will be created with the wrong owner.
# - run is required for the luigid pidfile and state
# - log is required for the luigid logs
RUN useradd --create-home luigi && \
    mkdir -p \
        docs/_build \
        log \
        run

# Copy the main source code
COPY . /usr/src/app

# Set privs on writable directories
RUN chown -R luigi:luigi docs/_build log run

# Switch to unprivileged user
USER luigi
