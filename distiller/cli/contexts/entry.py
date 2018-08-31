from distiller.cli.cli_context import CliContext

from distiller.cli.contexts.worker import context as worker_ctx
from distiller.cli.contexts.daemon import context as daemon_ctx
from distiller.cli.contexts.remote import context as remote_ctx

context = CliContext(
    "Main context of distiller CLI",
    sub_contexts={
        "daemon": daemon_ctx,
        "worker": worker_ctx,
        "remote": remote_ctx,
    }
)
