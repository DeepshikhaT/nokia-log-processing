# ─────────────────────────────────────────
# File    : test_transform.py
# Purpose : Unit tests for glue transformation logic
# Run     : python3 -m pytest tests/test_transform.py -v
# ─────────────────────────────────────────

import pytest
import re

# ─────────────────────────────────────────
# The same regex pattern used in glue_transform.py
# We test this pattern directly
# ─────────────────────────────────────────

PATTERN = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+(ERROR|INFO|WARN)\s+(.*)"

# Sample log lines for testing
VALID_ERROR_LINE = "2024-02-27 10:23:45 ERROR Payment service failed for user 1234"
VALID_INFO_LINE  = "2024-02-27 10:23:46 INFO Request received from 192.168.1.1"
VALID_WARN_LINE  = "2024-02-27 10:24:20 WARN Memory usage above 80% on server-01"
INVALID_LINE     = "This is a malformed log line with no pattern"
EMPTY_LINE       = ""


# ─────────────────────────────────────────
# Tests for timestamp extraction
# ─────────────────────────────────────────

def test_extract_timestamp_from_error_line():
    """Test timestamp is correctly extracted from ERROR line"""
    match = re.match(PATTERN, VALID_ERROR_LINE)
    assert match is not None
    assert match.group(1) == "2024-02-27 10:23:45"

def test_extract_timestamp_from_info_line():
    """Test timestamp is correctly extracted from INFO line"""
    match = re.match(PATTERN, VALID_INFO_LINE)
    assert match is not None
    assert match.group(1) == "2024-02-27 10:23:46"


# ─────────────────────────────────────────
# Tests for log level extraction
# ─────────────────────────────────────────

def test_extract_error_log_level():
    """Test ERROR log level is correctly extracted"""
    match = re.match(PATTERN, VALID_ERROR_LINE)
    assert match.group(2) == "ERROR"

def test_extract_info_log_level():
    """Test INFO log level is correctly extracted"""
    match = re.match(PATTERN, VALID_INFO_LINE)
    assert match.group(2) == "INFO"

def test_extract_warn_log_level():
    """Test WARN log level is correctly extracted"""
    match = re.match(PATTERN, VALID_WARN_LINE)
    assert match.group(2) == "WARN"


# ─────────────────────────────────────────
# Tests for message extraction
# ─────────────────────────────────────────

def test_extract_message_from_error_line():
    """Test message is correctly extracted from ERROR line"""
    match = re.match(PATTERN, VALID_ERROR_LINE)
    assert match.group(3) == "Payment service failed for user 1234"

def test_extract_message_from_info_line():
    """Test message is correctly extracted from INFO line"""
    match = re.match(PATTERN, VALID_INFO_LINE)
    assert match.group(3) == "Request received from 192.168.1.1"


# ─────────────────────────────────────────
# Tests for bad records handling
# ─────────────────────────────────────────

def test_malformed_line_returns_none():
    """Test that malformed line does not match pattern"""
    match = re.match(PATTERN, INVALID_LINE)
    assert match is None

def test_empty_line_returns_none():
    """Test that empty line does not match pattern"""
    match = re.match(PATTERN, EMPTY_LINE)
    assert match is None