# ─────────────────────────────────────────
# File    : test_upload.py
# Purpose : Unit tests for upload_to_s3.py
# Run     : pytest tests/test_upload.py
# ─────────────────────────────────────────

import pytest
import os
import sys
from datetime import datetime



sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))

from upload_to_s3 import get_date_partition, get_new_files

def test_date_partition_format():
    """Test that partition string has correct format"""
    result = get_date_partition()
    assert result.startswith("year=")
    assert "month=" in result
    assert "day=" in result



def test_date_partition_correct_year():
    """Test that partition contains current year"""
    result = get_date_partition()
    current_year = str(datetime.now().year)
    assert current_year in result

def test_date_partition_has_three_parts():
    """Test that partition has year month and day"""
    result = get_date_partition()
    parts = result.split("/")
    assert len(parts) == 3

def test_get_new_files_returns_list():
    """Test that function returns a list"""
    result = get_new_files('data/sample_logs')
    assert isinstance(result, list)

def test_get_new_files_only_log_files():
    """Test that only .log files are returned"""
    result = get_new_files('data/sample_logs')
    for file in result:
        assert file.endswith('.log')

def test_get_new_files_invalid_path():
    """Test that invalid path returns empty list"""
    result = get_new_files('data/nonexistent_folder')
    assert result == []