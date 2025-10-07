from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from onyx.configs.constants import DocumentSource
from onyx.connectors.host_files.connector import HostFilesConnector
from onyx.connectors.models import Document


def _collect_docs(
    batches: Iterable[list[Document]],
) -> list[Document]:
    documents: list[Document] = []
    for batch in batches:
        documents.extend(batch)
    return documents


@pytest.fixture
def patched_unstructured_key() -> MagicMock:
    with patch(
        "onyx.file_processing.extract_file_text.get_unstructured_api_key",
        return_value=None,
    ) as mock:
        yield mock


def test_host_files_connector_indexes_text_file(
    tmp_path: Path, patched_unstructured_key: MagicMock
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()

    file_path = root / "readme.txt"
    file_content = "Hello from Host Files!"
    file_path.write_text(file_content, encoding="utf-8")

    connector = HostFilesConnector(root_path=str(root))
    connector.load_credentials({})

    batches = list(connector.load_from_state())
    documents = _collect_docs(batches)

    assert len(documents) == 1
    doc = documents[0]

    assert doc.id.startswith("HOST_FILES__")
    assert doc.source == DocumentSource.HOST_FILES
    assert doc.semantic_identifier == "readme.txt"
    assert doc.sections
    assert doc.sections[0].text.strip() == file_content

    assert doc.metadata["relative_path"] == "readme.txt"
    assert doc.metadata["absolute_path"].endswith("readme.txt")

    file_mtime = datetime.fromtimestamp(
        file_path.stat().st_mtime, tz=timezone.utc
    )
    assert doc.doc_updated_at is not None
    assert abs((doc.doc_updated_at - file_mtime).total_seconds()) < 2


def test_host_files_connector_skips_hidden_files_by_default(
    tmp_path: Path, patched_unstructured_key: MagicMock
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()

    visible_file = root / "visible.txt"
    visible_file.write_text("visible", encoding="utf-8")

    hidden_file = root / ".hidden.txt"
    hidden_file.write_text("hidden", encoding="utf-8")

    connector = HostFilesConnector(root_path=str(root))
    batches = list(connector.load_from_state())
    documents = _collect_docs(batches)

    assert len(documents) == 1
    assert documents[0].metadata["relative_path"] == "visible.txt"


def test_host_files_connector_can_include_hidden_files(
    tmp_path: Path, patched_unstructured_key: MagicMock
) -> None:
    root = tmp_path / "workspace"
    root.mkdir()

    (root / "visible.txt").write_text("visible", encoding="utf-8")
    (root / ".hidden.txt").write_text("hidden", encoding="utf-8")

    connector = HostFilesConnector(root_path=str(root), allow_hidden=True)
    batches = list(connector.load_from_state())
    documents = _collect_docs(batches)

    relative_paths = sorted(doc.metadata["relative_path"] for doc in documents)
    assert relative_paths == [".hidden.txt", "visible.txt"]
