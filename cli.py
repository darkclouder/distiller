import sys

from distiller.cli.contexts.entry import context as entry_ctx


if __name__ == "__main__":
    entry_ctx.run((sys.argv[0],), sys.argv[1:])
