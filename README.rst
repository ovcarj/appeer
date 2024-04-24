appeer
===========================

(Experimental) code for getting statistics of the length of peer review. Currently, only `RSC <https://www.rsc.org/>`_ analysis is being implemented.

Installation
----------------------------------

The following installation procedure was tested:

.. code:: shell

        conda create -n appeer python=3.10
        conda activate appeer
        python -m pip install -e .

Usage
----------------------------------

A JSON file containing article URLs can be easily generated using `Publish or Perish <https://harzing.com/resources/publish-or-perish>`_. For an example JSON, check ``src/appeer/tests/sample_data/PoP.json``.

Article HTMLs are downloaded from the URLs stored in the JSON using:

.. code:: shell

        appeer_request -i <json_filename> -o <output_zip_archive_filename> -c

The HTMLs downloaded using the sample ``POP.json`` are given in ``src/appeer/tests/sample_data/htmls``.
