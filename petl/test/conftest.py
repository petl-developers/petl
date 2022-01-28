import logging


def pytest_configure():
    org = logging.Logger.debug

    def debug(self, msg, *args, **kwargs):
        org(self, str(msg), *args, **kwargs)

    logging.Logger.debug = debug
