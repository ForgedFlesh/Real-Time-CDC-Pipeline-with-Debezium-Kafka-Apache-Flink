from datetime import date, datetime
from decimal import Decimal

from scripts.validate import normalize, compare


def test_normalize_decimal():
    assert normalize(Decimal("1250.00")) == "1250.00"


def test_normalize_datetime():
    value = datetime(2026, 9, 16, 10, 30, 45, 123000)

    assert normalize(value) == "2026-09-16T10:30:45.123"


def test_normalize_date():
    value = date(2026, 9, 16)

    assert normalize(value) == "2026-09-16"


def test_compare_when_source_and_target_match():
    source = {
        1: (1, 1001, "PAID", "500.00"),
        2: (2, 1002, "CREATED", "750.00"),
    }

    target = {
        1: (1, 1001, "PAID", "500.00"),
        2: (2, 1002, "CREATED", "750.00"),
    }

    result = compare(source, target)

    assert result["source_count"] == 2
    assert result["target_count"] == 2
    assert result["missing_ids"] == []
    assert result["extra_ids"] == []
    assert result["mismatches"] == {}


def test_compare_detects_missing_row():
    source = {
        1: ("order-1",),
        2: ("order-2",),
    }

    target = {
        1: ("order-1",),
    }

    result = compare(source, target)

    assert result["missing_ids"] == [2]


def test_compare_detects_extra_row():
    source = {
        1: ("order-1",),
    }

    target = {
        1: ("order-1",),
        2: ("order-2",),
    }

    result = compare(source, target)

    assert result["extra_ids"] == [2]


def test_compare_detects_mismatch():
    source = {
        1: ("PAID", "500.00"),
    }

    target = {
        1: ("CREATED", "500.00"),
    }

    result = compare(source, target)

    assert 1 in result["mismatches"]