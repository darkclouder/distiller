Installation
============

Checkout repository and run ``pip install -r requirements.txt``

To further being able to run the demo tasks install pandas (run ``pip install pandas``).

You can use virtualenv to install the Distiller in a custom environment.

Configuration
-------------

There is a default configuration located at ``distiller/default_config.json``.
**Don't edit the default config, because this is used as a fallback for missing keys if the custom
config does not have any value set.**

To create an own config you can copy the default config, remove all keys you want to use from the default config
and alter all values you want to change.

TODO!!!!! DISTILLER_CONF_FILE is not used any more
The distiller loads the config file located at the position of the environment variable ``DISTILLER_CONF_FILE``, so
either set this variable to the path of your config file globally or within your virtual environment.

To specify an alternative path for the ``tasks`` folder that can be found in ``distiller/taks`` by default set the
environment variable ``DISTILLER_TASK_PATH``.

`Note: If you use file-based data drivers make sure the data path set in the environment variable`
``DISTILLER_DATA_PATH`` is set to a path that is synced across the network (at least across all workers).
`The distiller does not manage exchange of data. This is done by the drivers
(usually with a centralised database instance). Since files are not centralised the setup for syncing those
data folders is therefore left to the user.`
