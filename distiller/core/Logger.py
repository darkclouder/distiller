from enum import Enum
from datetime import datetime
import sys
import traceback


class LogLevel(Enum):
    DEBUG = 1
    NOTICE = 2
    WARNING = 3
    ERROR = 4
    CRITICAL = 5
    never = 9999


class Logger:
    def __init__(
        self,
        log,
        env
    ):
        self.log = log
        self.env = env

        self.verbose_level = LogLevel[self.env.config.get("log.verbose_level")]
        self.log_level = LogLevel[self.env.config.get("log.log_level")]
        self.exit_level = LogLevel[self.env.config.get("log.exit_level")]

    def claim(self, authority):
        return ClaimedLogger(self, authority)

    def _write_log(self, log_level, authority, message):
        str_message = "[%s](%s) %s: %s" % (log_level.name, datetime.now().__str__(), authority, message)

        if issubclass(message.__class__, Exception):
            traceback.print_exc()

        if log_level.value >= self.verbose_level.value:
            print(str_message, file=sys.stderr)

        if log_level.value >= self.log_level.value:
            self.log.write(str_message)

        if log_level.value >= self.exit_level.value:
            sys.exit(1)


class LoggerInterface:

    def _log(level, *args, **kwargs):
        raise NotImplementedError

    def debug(self, *args, **kwargs):
        return self._log(LogLevel.DEBUG, *args, **kwargs)

    def notice(self, *args, **kwargs):
        return self._log(LogLevel.NOTICE, *args, **kwargs)

    def warning(self, *args, **kwargs):
        return self._log(LogLevel.WARNING, *args, **kwargs)

    def error(self, *args, **kwargs):
        return self._log(LogLevel.ERROR, *args, **kwargs)

    def critical(self, *args, **kwargs):
        return self._log(LogLevel.CRITICAL, *args, **kwargs)


class ClaimedLogger(LoggerInterface):
    def __init__(self, logger, authority):
        self.logger = logger
        self.authority = authority

    def reclaim(self, authority):
        return self.logger.claim(authority)

    def _log(self, level, message):
        self.logger._write_log(level, self.authority, message)

    def catch(self, expected_exception):
        return CatchLogger(self, expected_exception)


class CatchLogger(LoggerInterface):
    def __init__(self, claimed_logger, expected_exception):
        self.claimed_logger = claimed_logger
        self.expected_exception = expected_exception

    def _log(self, level):
        return CatchLoggerCase(self.claimed_logger, self.expected_exception, level)


class CatchLoggerCase:
    def __init__(self, claimed_logger, expected_exception, level):
        self.claimed_logger = claimed_logger
        self.expected_exception = expected_exception
        self.level = level

    def __enter__(self):
        return None

    def __exit__(self, e_type, value, tb):
        if e_type is not None and issubclass(e_type, self.expected_exception):
            self.claimed_logger._log(
                self.level,
                "\n".join(traceback.format_exception(e_type, value, tb, 5))
            )

            return True
