"""Windows entry point for Spark's Python worker.

Spark launches ``python -m hktraffic.spark_worker`` (see ``get_spark``). On Windows the
worker talks to the JVM over a buffered socket and then the process exits. Python does not
flush that buffer on the way out, so the JVM reads EOF and reports that the worker crashed.
Flushing after every write makes the task result visible before the process ends.
"""
from __future__ import annotations

import os
import socket

_makefile = socket.socket.makefile


class _Flushing:
    def __init__(self, raw):
        self._raw = raw

    def write(self, data):
        written = self._raw.write(data)
        self._raw.flush()
        return written

    def __getattr__(self, name):
        return getattr(self._raw, name)


def _makefile_flushing(self, mode="r", buffering=None, *args, **kwargs):
    stream = _makefile(self, mode, buffering, *args, **kwargs)
    if "w" in mode:
        return _Flushing(stream)
    return stream


socket.socket.makefile = _makefile_flushing

from pyspark.java_gateway import local_connect_and_auth  # noqa: E402
from pyspark.serializers import write_int  # noqa: E402
from pyspark.worker import main  # noqa: E402

if __name__ == "__main__":
    port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    secret = os.environ["PYTHON_WORKER_FACTORY_SECRET"]
    sock_file, _sock = local_connect_and_auth(port, secret)
    write_int(os.getpid(), sock_file)
    sock_file.flush()
    main(sock_file, sock_file)
    sock_file.flush()
