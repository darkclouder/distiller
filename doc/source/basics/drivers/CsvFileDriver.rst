CsvFileDriver
=============

.. autoclass:: distiller.drivers.CsvFileDriver.CsvFileDriver

This file driver is used for creating csv files, stored at the path notation of a still
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
| dict                    | Read/write rows as dictionaries (see Python doc    | ``False``     |
|                         | `csv.DictReader`), if true also specify ``fields`` |               |
+-------------------------+----------------------------------------------------+---------------+
| fields                  | For ``dict=True``, list of columns for CSV file    | ``[]``        |
+-------------------------+----------------------------------------------------+---------------+
| csv_params              | Constructor parameters for ``csv.reader`` and      | ``{}``        |
|                         | ``csv.writer``, or ``csv.DictReader`` and          |               |
|                         | ``csv.DictWriter``                                 |               |
+-------------------------+----------------------------------------------------+---------------+

Examples
--------

To create a dict-based CsvFileDriver use:

::

  def stored_in(self):
      return CsvFileDriver(
          dict=True,
          fields=["lat", "lon", "name"],
          file_params={"encoding": "utf-8"}
      )


And then write dictionaries:

::

  with output_writer.replace() as writer:
      for bar in list_of_bars:
          lat, lon = bar["coordinates"]

          writer.write({
              "lat": lat,
              "lon": lon,
              "name": bar["name"],
          })

      writer.commit()
