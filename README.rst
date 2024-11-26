appeer
===========================

(Experimental) code for getting statistics of the length of peer review. Currently, only `RSC <https://www.rsc.org/>`_ analysis is being implemented.

Installation
----------------------------------

The following installation procedure was tested:

.. code:: shell

        conda create -n appeer python=3.11
        conda activate appeer
        python -m pip install -e .

After installation, run

.. code:: shell

        appeer init

To be able to run tests, use the following command: 

.. code:: shell

        python -m pip install -e .[test]

To run tests, simply type:

.. code:: shell

        pytest

Usage
----------------------------------

Article HTMLs can be downloaded from URLs stored in a file using:

.. code:: shell

        appeer scrape [OPTIONS] FILENAME

To see all ``appeer scrape`` options, use

.. code:: shell

        appeer scrape --help

The input file can be either a JSON file or a text file each URL written in a new line. The URLs can be written either as a full URL or a DOI. E.g., the following entries are equally valid:

.. code:: shell

          https://pubs.rsc.org/en/content/articlelanding/2023/ob/d3ob00424d
          10.1039/D3OB00424D

Invalid URL entries will be automatically skipped. For an example text file containing URLs, check ``src/appeer/tests/sample_data/example_publications.txt``. To download the HTMLs from this text file, simply use

.. code:: shell

        appeer scrape example_publications.txt

A JSON file containing article URLs can be easily generated using `Publish or Perish <https://harzing.com/resources/publish-or-perish>`_. For an example JSON, check ``src/appeer/tests/sample_data/example_PoP.json``.

The HTMLs downloaded using the sample ``example_POP.json`` are given in ``src/appeer/tests/sample_data/htmls``.

To clean various default ``appeer`` directories, type

.. code:: shell

        appeer clean --help
