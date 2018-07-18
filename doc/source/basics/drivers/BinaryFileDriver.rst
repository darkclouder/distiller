BinaryFileDriver
================

.. autoclass:: distiller.drivers.CsvFileDriver.CsvFileDriver

This file driver is used for creating raw/binary files, stored at the path notation of a still
in ``DISTILLER_DATA_PATH``.

Configuration
-------------
The configuration is done with the class's constructor in your task's ``stored_in``,
or in your configuration file with ``spirits.default_driver.params`` for kwargs.

+-------------------------+----------------------------------------------------+---------------+
| Parameter               | Description                                        | Default       |
+=========================+====================================================+===============+
| file_params             | Keyword arguments for `open()`, see Python doc     | ``{}``        |
+-------------------------+----------------------------------------------------+---------------+
| binary                  | Open file in binary mode and read/write bytes      | ``False``     |
|                         | instead of strings                                 |               |
+-------------------------+----------------------------------------------------+---------------+

Examples
--------

Write the stdout of a bash script to a file:

::

  from distiller.api.DefaultStill import DefaultStill
  from distiller.runners.BashRunner import BashRunner
  from distiller.drivers.BinaryFileDriver import BinaryFileDriver

  class Still(DefaultStill):
      def stored_in(self):
          return BinaryFileDriver(binary=True)

      def executed_by(self):
          return BashRunner("run.sh")

``run.sh``:

::

  #!/bin/bash

  echo "Hello World"
