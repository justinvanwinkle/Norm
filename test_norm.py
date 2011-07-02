import pytest

import norm

def f():
    raise SystemExit(1)

def test_mytest():
    x = {'a':'b', 'd': 'e'}
    y = {'a':'b', 'd': 'f'}
    assert x == y

