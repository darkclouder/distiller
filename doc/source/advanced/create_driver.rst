Create a driver
===============

A driver manages read and write operations of the stills.
The read and write interfaces are an abstraction to make the still as independent as
possible from the actual data source.

With a data driver class a new type of storage can be added, e.g. a database system
(like MongoDB, MySQL, ...), or other types of data storage or streaming.

The driver itself is still/spirit independent.

.. autoclass:: distiller.api.DataDriver.DataDriver
  :members:

The spirit is usually used to create a unique identifier on where to store the data (e.g. name of
a file, or database table).

``read`` has to return a reader:

.. autoclass:: distiller.api.Reader.Reader
  :members:

A reader is then an iterator:

.. autoclass:: distiller.api.Reader.ReadIterator
  :members:

  .. automethod:: __enter__
  .. automethod:: __exit__
  .. automethod:: __iter__


``write`` has to return a write mode selector:

.. autoclass:: distiller.api.Writer.WriteModes
  :members:

The write mode determines with which mode/strategy to write to the cache.

Each write mode returns the actual writer:

.. autoclass:: distiller.api.Writer.Writer
  :members:

  .. automethod:: __enter__
  .. automethod:: __exit__

To look into an implementation see:

.. autoclass:: distiller.drivers.CsvFileDriver.CsvFileDriver
