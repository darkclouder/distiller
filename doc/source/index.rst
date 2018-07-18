.. Distiller documentation master file, created by
   sphinx-quickstart on Sat Mar 24 11:14:38 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Distiller's documentation!
=====================================

The distiller is a dependency management, data management, job scheduling and job distribution system
for data scientists. Its aim is to automate and manage the execution of data-focused batch jobs and keep
its data up-to-date while keeping the batch execution to a minimum with lazy execution.


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   usage/installation
   usage/quickstart
   usage/cli

   basics/drivers/CsvFileDriver
   basics/drivers/BinaryFileDriver

   basics/runners/PythonRunner
   basics/runners/BashRunner

   advanced/create_task
   advanced/create_driver
   advanced/create_runner



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
