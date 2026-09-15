"""Direct PID-1 entrypoint with cooperative signals and dual-output logging.

No child shell. Signals only set a flag, so they cannot raise in
the middle of resource acquisition or interrupt cleanup a second time.
"""
import argparse
from contextlib import contextmanager
import os
import signal
import subprocess
import sys


class ShutdownRequest:
    def __init__(self):
        self.requested = False

    def receive(self, signum, frame):
        # No locks, I/O, network operations or exceptions in the signal handler.
        self.requested = True


@contextmanager
def shutdown_signals():
    request = ShutdownRequest()
    previous = {}
    try:
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, request.receive)
        yield request
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


@contextmanager
def tee_output(log_path):
    # Preserve the existing tee's byte-level behavior, including native SDK
    # diagnostics on fd 2. Python, not a shell, owns PID 1 and reaps the logger.
    with open(log_path, 'ab'):
        pass
    sys.stdout.flush()
    sys.stderr.flush()
    stdout, stderr = os.dup(1), os.dup(2)
    logger = None
    try:
        logger = subprocess.Popen(['tee', '-a', log_path], stdin=subprocess.PIPE,
                                  stdout=stdout, stderr=stderr)
        os.dup2(logger.stdin.fileno(), 1)
        os.dup2(logger.stdin.fileno(), 2)
        yield
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        finally:
            os.dup2(stdout, 1)
            os.dup2(stderr, 2)
            os.close(stdout)
            os.close(stderr)
            if logger is not None:
                logger.stdin.close()
                try:
                    code = logger.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.kill()
                    logger.wait()
                    raise RuntimeError('Config log drain timed out')
                if code:
                    raise RuntimeError('Config log drain failed')


def run_service(entry, log_path):
    with shutdown_signals() as shutdown:
        with tee_output(log_path):
            try:
                return entry(shutdown) if not shutdown.requested else 0
            except Exception:
                import traceback
                traceback.print_exc()
                return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serviceAccount', required=True)
    parser.add_argument('--log-file', default='/app/logs/config_sync.log')
    args = parser.parse_args()

    def entry(shutdown):
        # Install signal handling before the comparatively slow SDK imports.
        if __package__:
            from .config_sync_listener import main as listen
        else:
            from config_sync_listener import main as listen
        return listen(args.serviceAccount, shutdown=shutdown)

    return run_service(entry, args.log_file)


if __name__ == '__main__':
    sys.exit(main())
