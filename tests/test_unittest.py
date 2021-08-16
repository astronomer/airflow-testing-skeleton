"""Demonstration of testing with unittest library."""

from unittest import TestCase


def multiply(a, b):
    return a * b


class DemoUnitTest(TestCase):
    def setUp(self):
        self.a = 2
        self.b = 3

    def test_multiply_a(self):
        assert multiply(self.a, 2) == 4

    def test_multiply_b(self):
        assert multiply(self.b, 2) == 6

    def test_multiply_ab(self):
        assert multiply(self.a, self.b) == 6
