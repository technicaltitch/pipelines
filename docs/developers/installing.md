# Installing a new Pipeline environment

Data Pipelines typically make heavy use of the ecosystem of Python data science libraries.
Many of these libraries require the compilation of C code as part of the setup which makes
them more difficult to install, especially on non-Linux enviroments. Anaconda is a Python
distribution focused on data science and it contains the `conda` package manager which
can install pre-compiled binary versions of most data science packages. Consequently,
Pipelines projects should use the Anaconda Python distribution in preference to the 
standard Python distribution for the operating system.

There are some packages that are not available in the standard Anaconda package repository,
but which are available in the community-maintained *Conda Forge* repository. The packages
in this repository are of variable quality, but it is a useful secondary source for
critical packages, notably *GDAL*. The `conda install` command below uses `--channel`
to ensure that packages can be installed from *Conda Forge*, but only if there isn't a
matching package in the default repository.

1. Install Miniconda (the base system and package manager):

    ```
    wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh
    ```

   Note that data analysts may wish to install the full Anaconda distribution instead.

2. Create a `conda` environment for the project:

    ```
    conda create --name <project name>
    ```

3. Activate the environment:

    ```
    source activate <project name>
    ```

4. Install the base requirements for the project (enable the `conda-forge` channel to ensure we can install GDAL, etc.):

    ```
    conda install --channel defaults --channel conda-forge --file requirements/base.conda
    ```

5. Re-activate the environment, because the `base.conda` file might have specified a new Python version:

    ```
    source activate <project name>
    ```

6. Install non-conda requirements:

    ```
    pip install --requirement requirements/base.pip
    ```

7. Use `pip` or `conda` to install additional packages that are useful but not part of the core requirements.
For example:

    ```
    pip install docker-compose    # Required by developers maintaining the Gitlab-CI configuration
    ```
