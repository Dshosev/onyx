from __future__ import annotations

import fnmatch
import hashlib
import mimetypes
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Iterator

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.exceptions import ConnectorValidationError
from onyx.connectors.file.connector import _process_file
from onyx.connectors.interfaces import GenerateDocumentsOutput
from onyx.connectors.interfaces import LoadConnector
from onyx.connectors.models import Document
from onyx.utils.logger import setup_logger


logger = setup_logger()


class HostFilesConnector(LoadConnector):
    """Connector that scans a host filesystem directory and indexes supported files."""

    DOC_ID_PREFIX = "HOST_FILES__"

    def __init__(
        self,
        root_path: str,
        *,
        allow_hidden: bool = False,
        follow_symlinks: bool = False,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
        batch_size: int = INDEX_BATCH_SIZE,
    ) -> None:
        self.root_path = Path(root_path).expanduser()
        self.allow_hidden = allow_hidden
        self.follow_symlinks = follow_symlinks
        self.include_patterns = list(include_patterns) if include_patterns else None
        self.exclude_patterns = list(exclude_patterns) if exclude_patterns else []
        self.batch_size = batch_size
        self.pdf_pass: str | None = None
        self._resolved_root: Path | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        # Matches the LocalFileConnector interface to support encrypted PDFs.
        self.pdf_pass = credentials.get("pdf_password")
        return None

    def validate_connector_settings(self) -> None:
        self._ensure_root_path(strict=True)

    def _ensure_root_path(self, *, strict: bool = False) -> Path:
        if self._resolved_root is not None:
            return self._resolved_root

        try:
            resolved = self.root_path.resolve(strict=strict)
        except FileNotFoundError as exc:
            raise ConnectorValidationError(
                f"Configured root_path '{self.root_path}' does not exist."
            ) from exc

        if not resolved.exists():
            if strict:
                raise ConnectorValidationError(
                    f"Configured root_path '{self.root_path}' does not exist."
                )
            logger.warning(
                "Configured root_path '%s' does not exist yet; skipping indexing.",
                self.root_path,
            )
            resolved = self.root_path

        if resolved.exists() and not resolved.is_dir():
            raise ConnectorValidationError(
                f"Configured root_path '{self.root_path}' is not a directory."
            )

        self._resolved_root = resolved
        return resolved

    @staticmethod
    def _has_hidden_component(parts: tuple[str, ...]) -> bool:
        return any(part.startswith(".") for part in parts)

    def _matches_patterns(self, relative_path: Path) -> bool:
        relative_str = relative_path.as_posix()

        if self.exclude_patterns and any(
            fnmatch.fnmatch(relative_str, pattern)
            for pattern in self.exclude_patterns
        ):
            return False

        if not self.include_patterns:
            return True

        return any(
            fnmatch.fnmatch(relative_str, pattern)
            for pattern in self.include_patterns
        )

    def _iter_candidate_files(self) -> Iterator[Path]:
        root = self._ensure_root_path()
        if not root.exists():
            return

        # os.walk is used instead of Path.rglob to allow pruning hidden directories.
        for dirpath, dirnames, filenames in os.walk(
            root, followlinks=self.follow_symlinks
        ):
            current_dir = Path(dirpath)
            try:
                relative_dir = current_dir.relative_to(root)
                relative_parts = relative_dir.parts
            except ValueError:
                relative_parts = ()

            if not self.allow_hidden:
                dirnames[:] = [
                    dirname
                    for dirname in dirnames
                    if not self._has_hidden_component(relative_parts + (dirname,))
                ]

            for filename in filenames:
                path = current_dir / filename
                try:
                    relative_path = path.relative_to(root)
                except ValueError:
                    # Should not happen, but skip defensively.
                    continue

                if (
                    not self.allow_hidden
                    and self._has_hidden_component(relative_path.parts)
                ):
                    continue

                if not self._matches_patterns(relative_path):
                    continue

                if not path.is_file():
                    continue

                yield path

    def _build_file_id(self, path: Path, root: Path) -> str:
        try:
            relative_path = path.relative_to(root)
        except ValueError:
            relative_path = path

        unique_key = f"{root.as_posix()}::{relative_path.as_posix()}"
        return hashlib.sha256(unique_key.encode("utf-8")).hexdigest()

    def _build_metadata(self, path: Path, root: Path) -> dict[str, Any]:
        stat_result = path.stat()
        last_modified = datetime.fromtimestamp(stat_result.st_mtime, tz=timezone.utc)
        try:
            relative_path = path.relative_to(root)
        except ValueError:
            relative_path = path

        return {
            "connector_type": DocumentSource.HOST_FILES.value,
            "doc_updated_at": last_modified.isoformat(),
            "relative_path": relative_path.as_posix(),
            "absolute_path": str(path.resolve()),
        }

    def _guess_mime_type(self, path: Path) -> str | None:
        mime_type, _ = mimetypes.guess_type(path.as_posix())
        return mime_type

    def load_from_state(self) -> GenerateDocumentsOutput:
        documents: list[Document] = []
        root = self._ensure_root_path()

        if not root.exists():
            logger.warning(
                "Skipping HostFilesConnector run because root_path '%s' does not exist.",
                self.root_path,
            )
            return

        for file_path in self._iter_candidate_files():
            file_id = self._build_file_id(file_path, root)
            metadata = self._build_metadata(file_path, root)
            mime_type = self._guess_mime_type(file_path)

            try:
                with file_path.open("rb") as file_stream:
                    processed_docs = _process_file(
                        file_id=file_id,
                        file_name=file_path.name,
                        file=file_stream,
                        metadata=metadata,
                        pdf_pass=self.pdf_pass,
                        file_type=mime_type,
                    )
            except Exception as exc:
                logger.warning("Failed to process file '%s': %s", file_path, exc)
                continue

            if not processed_docs:
                continue

            for doc in processed_docs:
                doc.id = f"{self.DOC_ID_PREFIX}{file_id}"
                doc.source = DocumentSource.HOST_FILES
                documents.append(doc)

            if len(documents) >= self.batch_size:
                yield documents
                documents = []

        if documents:
            yield documents
