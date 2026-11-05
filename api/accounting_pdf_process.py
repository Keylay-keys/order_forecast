"""Bound child output while reading it, with whole-group timeout supervision."""
import os
import selectors
import signal
import subprocess
import time


def run_bounded(command, *, timeout, stdout_limit, new_session=False):
    # Do not inherit database, Firebase, loader, Python-path or home credentials.
    environment = {"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8"}
    result = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": stdout_limit, "stderr": 64 * 1024}
    deadline = time.monotonic() + timeout
    with subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, close_fds=True, env=environment,
                          start_new_session=new_session) as process:
        try:
            with selectors.DefaultSelector() as selector:
                for name in result:
                    stream = getattr(process, name)
                    os.set_blocking(stream.fileno(), False)
                    selector.register(stream, selectors.EVENT_READ, name)
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise subprocess.TimeoutExpired(command, timeout)
                    for key, _ in selector.select(remaining):
                        name = key.data
                        chunk = os.read(key.fd, min(65536, limits[name] - len(result[name]) + 1))
                        if not chunk:
                            selector.unregister(key.fileobj)
                            continue
                        result[name].extend(chunk)
                        if len(result[name]) > limits[name]:
                            raise ValueError("Reader output limit")
            returncode = process.wait(timeout=max(.001, deadline - time.monotonic()))
            if returncode:
                # Never include untrusted PDF output in an exception or log.
                raise subprocess.CalledProcessError(returncode, command)
            return bytes(result["stdout"])
        except BaseException:
            try:
                if new_session:
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    # The native sandbox forbids creating any descendants.
                    process.kill()
            except ProcessLookupError:
                pass
            process.wait()
            raise
