Installation
============

Checkout the Distiller repository.

Make sure Python 3.6 or above is installed and both ``python`` and ``pip`` refer to the Python 3.6 instance.
You can check that with ``python --version`` and ``pip --version``.
Otherwise, use ``python3`` and ``pip3`` instead of ``python`` and ``pip`` or create a virtual environment
with ``virtualenv --python=`which python` .`` and activate it with ``source bin/activate``.

Then run ``pip install -r requirements.txt``

To further being able to run the demo tasks install overpass (run ``pip install overpass``).

Configuration
-------------

There is a default configuration located at ``distiller/default_config``
for daemon settings, worker settings (also used by remote CLI) and global settings (for both worker and daemon).
**Don't edit the default config, because this is used as a fallback for missing keys if the custom
config does not have any value.**

To create an own config you can copy the default configs to a new directory,
remove all keys you want to use from the default configurations and alter all values you want to change.

The distiller loads the config files located in the directory of the environment variable ``DISTILLER_CONF_PATH``.
In any case, the default configuration is sued as a fallback.

To specify an alternative path for the ``tasks`` folder that can be found in ``distiller/tasks`` by default set the
environment variable ``DISTILLER_TASK_PATH``.

`Note: If you use file-based data drivers with distributed workers make sure the data path set in the environment variable`
``DISTILLER_DATA_PATH`` is set to a path that is synced across the network (at least across all workers).
`The distiller does not manage exchange of data. This is done by the drivers
(usually with a centralised database instance). Since files are not centralised, the setup for syncing those
data folders is therefore left to the user.`
