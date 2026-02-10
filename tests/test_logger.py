from __future__ import annotations
from datetime import datetime
from unittest.mock import patch
import pytest
from peercolab_engine import LogMessage, LogLevel, Logger, DefaultLogger


class TestLogMessage:
    def test_is_within_filters_by_level(self):
        msg = LogMessage("src", datetime.now(), LogLevel.ERROR, "err")
        assert msg.is_within(LogLevel.ERROR) is True
        assert msg.is_within(LogLevel.FATAL) is False
        assert msg.is_within(LogLevel.DEBUG) is True

    def test_to_string_includes_timestamp_and_level(self):
        d = datetime(2024, 1, 1, 12, 30, 45, 123000)
        msg = LogMessage("src", d, LogLevel.INFO, "hello")
        s = str(msg)
        assert "12:30:45.123" in s
        assert "INFO" in s
        assert "hello" in s

    def test_to_string_includes_error_when_present(self):
        msg = LogMessage("src", datetime.now(), LogLevel.ERROR, "msg", RuntimeError("boom"))
        s = str(msg)
        assert "boom" in s

    def test_to_json_returns_same_as_to_string(self):
        msg = LogMessage("src", datetime.now(), LogLevel.INFO, "test")
        assert msg.to_json() == str(msg)


class TestLogger:
    def setup_method(self):
        Logger.assign_logger(DefaultLogger())

    def test_write_at_all_levels_without_throwing(self):
        Logger.trace("trace msg")
        Logger.debug("debug msg")
        Logger.info("info msg")
        Logger.warning("warning msg")
        Logger.error("error msg")
        Logger.fatal("fatal msg")

    def test_write_with_error_passes_exception(self):
        messages = []

        class CaptureLogger:
            log_level = LogLevel.TRACE

            def write(self, msg):
                messages.append(msg)

        Logger.assign_logger(CaptureLogger())
        err = RuntimeError("test error")
        Logger.error("something failed", err)
        assert len(messages) == 1
        assert "test error" in str(messages[0])

    def test_update_source_sets_source(self):
        Logger.update_source("my-service")

    def test_assign_logger_uses_custom_logger(self):
        messages = []

        class CaptureLogger:
            log_level = LogLevel.TRACE

            def write(self, msg):
                messages.append(str(msg))

        Logger.assign_logger(CaptureLogger())
        Logger.info("custom message")
        assert len(messages) > 0
        assert "custom message" in messages[0]


class TestLogLevelOrdering:
    def test_levels_are_ordered(self):
        assert LogLevel.FATAL == 0
        assert LogLevel.ERROR == 1
        assert LogLevel.WARNING == 2
        assert LogLevel.INFO == 3
        assert LogLevel.DEBUG == 4
        assert LogLevel.TRACE == 5
        assert LogLevel.FATAL < LogLevel.ERROR
        assert LogLevel.ERROR < LogLevel.WARNING
        assert LogLevel.WARNING < LogLevel.INFO
        assert LogLevel.INFO < LogLevel.DEBUG
        assert LogLevel.DEBUG < LogLevel.TRACE


class TestDefaultLogger:
    def test_writes_when_within_level(self, capsys):
        logger = DefaultLogger()
        msg = LogMessage("src", datetime.now(), LogLevel.ERROR, "error msg")
        logger.write(msg)
        captured = capsys.readouterr()
        assert "error msg" in captured.out

    def test_does_not_write_when_below_level(self, capsys):
        logger = DefaultLogger()
        logger.log_level = LogLevel.ERROR
        msg = LogMessage("src", datetime.now(), LogLevel.INFO, "info msg")
        logger.write(msg)
        captured = capsys.readouterr()
        assert captured.out == ""
