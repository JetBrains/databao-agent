import os

import pytest

from portus._config_utils import expand_env_vars_str


def test_no_placeholders_returns_input_unchanged() -> None:
    s = "/path/with/no/placeholders"
    assert expand_env_vars_str(s) == s


def test_single_variable_substitution(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HOME", "/home/user")
    assert expand_env_vars_str("${HOME}") == "/home/user"
    assert expand_env_vars_str("prefix/${HOME}/suffix") == "prefix//home/user/suffix"


def test_multiple_different_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FOO", "foo")
    monkeypatch.setenv("BAR", "bar")
    assert expand_env_vars_str("${FOO}-${BAR}") == "foo-bar"


def test_repeated_same_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("X", "42")
    assert expand_env_vars_str("${X}/${X}/${X}") == "42/42/42"


def test_variable_not_set_raises_keyerror(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure var is not present
    if "MISSING_VAR" in os.environ:
        monkeypatch.delenv("MISSING_VAR", raising=False)
    with pytest.raises(KeyError):
        expand_env_vars_str("prefix/${MISSING_VAR}/suffix")


def test_mixture_of_supported_and_unsupported_syntax(monkeypatch: pytest.MonkeyPatch) -> None:
    # Only ${VAR} should expand; $VAR should remain unchanged
    monkeypatch.setenv("FOO", "value")
    assert expand_env_vars_str("a/${FOO}/b:$FOO") == "a/value/b:$FOO"


def test_empty_variable_name_raises_keyerror(monkeypatch: pytest.MonkeyPatch) -> None:
    # Pattern matches ${} -> empty name, which should raise
    with pytest.raises(KeyError):
        expand_env_vars_str("start/${}/end")
