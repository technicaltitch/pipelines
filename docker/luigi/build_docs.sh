#!/bin/bash
set -e

# Additional packages are required to be able to produce PDF documentation (note that these are a 700MB download)
#apt-get update && apt-get install -y texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk

# Build the documentation
cd docs

make html
make latexpdf

# Copy build pdf docs to the html folder so they can be downloaded from hosted documentation
cp _build/latex/*.pdf _build/html/

cd ..
