appeer
===========================

(Experimental) code for scraping scientific journals to get statistics on the length of peer review. Currently, only `RSC <https://www.rsc.org/>`_ analysis is being implemented.

**Usage**

A JSON file containing article URLs can be easily generated using `Publish or Perish <https://harzing.com/resources/publish-or-perish>`_. For an example JSON, check ``examples/PoP.json``.

Article HTMLs are downloaded from the URLs stored in the JSON using:

.. code:: shell

        python request.py <json_filename>

The HTMLs downloaded using ``examples/POP.json`` can be seen in ``examples/example_htmls``.
