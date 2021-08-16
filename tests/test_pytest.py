"""Demonstration of testing with pytest library."""

import pytest


def multiply(a, b):
    return a * b


@pytest.fixture
def a():
    return 2


@pytest.fixture
def b():
    return 3


@pytest.fixture
def ab(a, b):
    return multiply(a, b)


def test_multiply_a(a):
    assert multiply(a, 2) == 4


def test_multiply_b(b):
    assert multiply(b, 2) == 6


def test_multiply_ab(a, b):
    assert multiply(a, b) == 6


def test_multiply_ab_b(ab, b):
    assert multiply(ab, b) == 18
