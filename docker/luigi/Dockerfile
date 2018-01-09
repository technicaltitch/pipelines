FROM python:3.6

RUN apt-get update && \
    apt-get install -y \
        apt-transport-https \
        vim \
        curl \
        postgresql-client \
        build-essential \
        git-core \
        gdal-bin \
        libssl-dev \
        libffi-dev \
        libsasl2-dev \
        libldap2-dev \
        libhdf5-serial-dev \
        libproj-dev \
        libgdal1-dev \
        libxml2-dev \
        libxslt1-dev && \
    apt-get clean && \
    apt-get -y autoremove && \ 
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

WORKDIR /usr/src/app

# Install Python dependencies
# Make sure editable installs aren't in the app src directory (because then they
# are no longer available if we mount the upstream source directly)
COPY requirements requirements
RUN pip install --no-cache-dir --upgrade setuptools pip
RUN pip install --src /usr/src --no-cache-dir -r requirements/production.pip

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

