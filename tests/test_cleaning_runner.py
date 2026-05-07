import logging
import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.ingestion.cleaning import runner


def _close_handlers(handlers: list[logging.Handler]) -> None:
    for handler in handlers:
        handler.close()


def test_build_log_handlers_skips_file_handler_when_disabled(monkeypatch):
    monkeypatch.setenv("LOG_TO_FILE", "false")

    handlers = runner.build_log_handlers()
    try:
        assert len(handlers) == 1
        assert isinstance(handlers[0], logging.StreamHandler)
        assert not any(isinstance(handler, logging.FileHandler) for handler in handlers)
    finally:
        _close_handlers(handlers)


def test_write_text_atomic_removes_tmp_file_when_replace_fails(monkeypatch):
    runtime_dir = Path.cwd() / "tests" / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    target_file = runtime_dir / f"write-atomic-{uuid.uuid4().hex}.txt"
    temp_file = target_file.with_suffix(".tmp")

    def _raise_replace(self: Path, target: Path) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(Path, "replace", _raise_replace)

    with pytest.raises(OSError, match="simulated replace failure"):
        runner.write_text_atomic(target_file, "Điều 1. Test")

    assert not temp_file.exists()
    assert not target_file.exists()
