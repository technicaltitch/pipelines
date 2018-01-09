# Documentation

## Introduction

All Pipelines require documentation to explain the complete process followed
to analyze the data to allow quality assurance and ensure replicability.

## Tools

The documentation for Pipelines is based around [Sphinx](http://www.sphinx-doc.org/en/stable/index.html).
Sphinx takes documentation in a various plain text markup formats and
compiles it into production quality documentation in different output formats.

Kimetrica uses the following input markup formats:

* [Markdown](http://commonmark.org/help/) is a simple and widely-used markup language. It is the default
markup language used within Kimetrica.
* [reStructuredText](http://docutils.sourceforge.net/docs/user/rst/quickref.html) is a more sophisticated
but less widely-ised markup language. It provides many more features than Markdown for dealing
with cross-references, footnotes, embedded diagrams, etc. It should be used where the document
needs features not easily achieved using Markdown.
* [LaTeX](https://www.sharelatex.com/learn/Mathematical_expressions) is a high-quality typesetting system
widely used in scientific publishing but with a steeper learning curve than reStructuredText. It can
be embedded within reStructuredText documents to include mathematical expressions and other advanced
layout features.

Sphinx renders the combined input into multiple outputs:

* HTML web pages suitable for use as online help or documentation
* PDF documents suitable for use as client deliverables

## Using Sphinx

### Required operating system packages

In order to produce PDF files on Ubuntu you need to install the following packages
(note that these are a 700MB download):

```
sudo apt-get install texlive-latex-recommended texlive-fonts-recommended texlive-latex-extra latexmk
```

### Manually compiling documentation

1. Change to the `pipelines/docs` directory, e.g. `cd pipelines/docs`
2. Run `make` with the required *target*. For example:

    * `make html`
    * `make latexpdf`

### Automatically compiling HTML documentation

In order to make it easier to see changes as you update the documentation,
you can tell Sphinx to automatically update the HTML documentation as the
source files are updated:


1. Change to the `pipelines/docs` directory, e.g. `cd pipelines/docs`
2. Run `make livehtml`

This will open a new browser window pointing to a local web server running
on port 8000 (by default) with livereload enabled, allowing changes in
the source files to be automatically complied and displayed.