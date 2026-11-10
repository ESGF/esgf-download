from __future__ import annotations

import asyncio
import hashlib
import os
from collections.abc import AsyncIterator

import httpx
import pytest

from esgpull.config import Config
from esgpull.download import (
    DownloadCtx,
    build_range_header,
    content_range_ok,
    parse_content_range,
)
from esgpull.exceptions import DownloadSizeError, RangeNoProgressError
from esgpull.fs import FileCheck, Filesystem
from esgpull.models import File
from esgpull.processor import Task
from esgpull.result import Err, Ok, Result


def make_file(size: int = 3000) -> tuple[File, bytes]:
    data = os.urandom(size)
    file = File(
        file_id="file",
        dataset_id="dataset",
        master_id="master",
        url="https://example.org/file.nc",
        version="v0",
        filename="file.nc",
        local_path="project/folder",
        data_node="data_node",
        checksum=hashlib.sha256(data).hexdigest(),
        checksum_type="SHA256",
        size=size,
    )
    file.compute_sha()
    return file, data


class BodyStream(httpx.AsyncByteStream):
    """
    Async response body, optionally raising as if the server had
    closed the connection mid-body (curl error 18 equivalent).
    """

    def __init__(self, chunks: list[bytes], truncated: bool = False):
        self._chunks = chunks
        self._truncated = truncated

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for chunk in self._chunks:
            yield chunk
        if self._truncated:
            raise httpx.RemoteProtocolError(
                "peer closed connection without sending complete"
                " message body"
            )

    async def aclose(self) -> None:
        pass


def run_task(
    task: Task, transport: httpx.MockTransport
) -> list[Result[DownloadCtx]]:
    async def _run() -> list[Result[DownloadCtx]]:
        results: list[Result[DownloadCtx]] = []
        async with httpx.AsyncClient(transport=transport) as client:
            async for result in task.stream(
                asyncio.Semaphore(1), client
            ):
                results.append(result)
        return results

    return asyncio.run(_run())


def recorded_ranges(requests: list[httpx.Request]) -> list[str | None]:
    return [req.headers.get("Range") for req in requests]


def content_range(start: int, end: int, total: int) -> dict[str, str]:
    return {"Content-Range": f"bytes {start}-{end}/{total}"}


@pytest.fixture
def fs(config: Config) -> Filesystem:
    return Filesystem.from_config(config, install=True)


# ---------------------------------------------------------------- helpers


def test_build_range_header():
    assert build_range_header(0, None) == "bytes=0-"
    assert build_range_header(1000, None) == "bytes=1000-"
    assert build_range_header(1024, 2047) == "bytes=1024-2047"


def test_parse_content_range():
    assert parse_content_range("bytes 1000-1999/3000") == (1000, 1999, 3000)
    assert parse_content_range("bytes 0-9/*") == (0, 9, None)
    assert parse_content_range("BYTES 0-9/10") == (0, 9, 10)
    assert parse_content_range(None) is None
    assert parse_content_range("garbage") is None
    assert parse_content_range("items 0-9/10") is None
    assert parse_content_range("bytes x-y/10") is None
    assert parse_content_range("bytes 0-9") is None


def test_content_range_ok():
    assert content_range_ok("bytes 1000-2999/3000", 1000, 3000)
    # missing / malformed header
    assert not content_range_ok(None, 0, 10)
    assert not content_range_ok("garbage", 0, 10)
    # start mismatch
    assert not content_range_ok("bytes 0-9/10", 5, 10)
    # total mismatch with a known expected size
    assert not content_range_ok("bytes 0-9/999", 0, 10)
    # lenient: unknown total or unknown expected size
    assert content_range_ok("bytes 0-9/*", 0, 10)
    assert content_range_ok("bytes 0-9/999", 0, 0)
    # lenient: `end` is not checked (extra in-order bytes are harmless)
    assert content_range_ok("bytes 0-99/100", 0, 100)


# ------------------------------------------------------------- downloader


def test_truncated_response_keeps_part_bytes(config: Config, fs: Filesystem):
    file, data = make_file(6000)
    # httpx buffers raw bytes until a full `chunk_size` is available,
    # so only chunks yielded before the truncation survive on disk.
    config.download.chunk_size = 1000
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        # Node cuts the response early, as esgf-optimesm does at ~1.1 GB.
        return httpx.Response(200, stream=BodyStream([data[:4000]], True))

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert len(results) > 0
    assert isinstance(results[-1], Err)
    assert isinstance(results[-1].err, httpx.RemoteProtocolError)
    # The regression that matters: bytes survive on disk for a resume.
    part = fs[file].tmp
    assert part.is_file()
    assert part.stat().st_size == 4000
    assert part.read_bytes() == data[:4000]


def test_resume_from_partial_completes_and_finalizes(
    config: Config, fs: Filesystem
):
    file, data = make_file(3000)
    fs[file].tmp.write_bytes(data[:1000])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        start = int(request.headers["Range"].split("=")[1].rstrip("-"))
        return httpx.Response(
            206,
            content=data[start:],
            headers=content_range(start, len(data) - 1, len(data)),
        )

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == ["bytes=1000-"]
    assert isinstance(results[-1], Ok)
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)
    assert fs[file].drs.read_bytes() == data


def test_resume_zero_byte_part_is_plain_get(
    config: Config, fs: Filesystem
):
    file, data = make_file(3000)
    fs[file].tmp.write_bytes(b"")
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=data)

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == [None]
    assert isinstance(results[-1], Ok)
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)
    assert fs[file].drs.read_bytes() == data


def test_range_size_sequential_windows(config: Config, fs: Filesystem):
    config.download.range_size = 1024
    file, data = make_file(3000)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        spec = request.headers["Range"].removeprefix("bytes=")
        start_s, _, end_s = spec.partition("-")
        start, end = int(start_s), int(end_s)
        return httpx.Response(
            206,
            content=data[start : end + 1],
            headers=content_range(start, end, len(data)),
        )

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == [
        "bytes=0-1023",
        "bytes=1024-2047",
        "bytes=2048-2999",
    ]
    assert isinstance(results[-1], Ok)
    assert results[-1].data.completed == 3000
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)
    assert fs[file].drs.read_bytes() == data


def test_range_size_larger_than_file(config: Config, fs: Filesystem):
    config.download.range_size = 100_000
    file, data = make_file(3000)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            206,
            content=data,
            headers=content_range(0, 2999, 3000),
        )

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == ["bytes=0-2999"]
    assert isinstance(results[-1], Ok)


def test_range_ignored_200_falls_back_to_full_get(
    config: Config, fs: Filesystem
):
    config.download.range_size = 1024
    file, data = make_file(3000)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        # Server silently ignores the Range header.
        return httpx.Response(200, content=data)

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == ["bytes=0-1023", None]
    assert isinstance(results[-1], Ok)
    assert results[-1].data.completed == 3000
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)
    assert fs[file].drs.read_bytes() == data


def test_range_fallback_truncates_stale_part(
    config: Config, fs: Filesystem
):
    # Resume starts (bytes=1000-), server ignores Range and sends the
    # full body: the stale `.part` must be truncated before appending.
    file, data = make_file(3000)
    fs[file].tmp.write_bytes(data[:1000])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=data)

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == ["bytes=1000-", None]
    assert isinstance(results[-1], Ok)
    assert results[-1].data.completed == 3000
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)
    assert fs[file].drs.read_bytes() == data


def test_content_range_start_mismatch_falls_back(
    config: Config, fs: Filesystem
):
    file, data = make_file(3000)
    fs[file].tmp.write_bytes(data[:1000])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.headers.get("Range") is not None:
            # 206, but the server echoes the wrong span start.
            return httpx.Response(
                206,
                content=data[1000:],
                headers=content_range(0, 2999, 3000),
            )
        return httpx.Response(200, content=data)

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == ["bytes=1000-", None]
    assert isinstance(results[-1], Ok)
    digest = results[-1].data.digest
    assert digest is not None
    assert fs.finalize(file, digest=digest) == Ok(FileCheck.Ok)


def test_empty_206_response_errors(config: Config, fs: Filesystem):
    file, data = make_file(3000)
    fs[file].tmp.write_bytes(data[:1000])

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            content=b"",
            headers=content_range(1000, 2999, 3000),
        )

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert isinstance(results[-1], Err)
    assert isinstance(results[-1].err, RangeNoProgressError)
    # No progress was made, nothing was appended.
    assert fs[file].tmp.stat().st_size == 1000


def test_resume_disabled_for_unknown_size(
    config: Config, fs: Filesystem
):
    file, data = make_file(3000)
    file.size = 0  # unknown size (e.g. some index records)
    fs[file].tmp.write_bytes(data[:1000])
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=data)

    task = Task(config, fs, file=file)
    results = run_task(task, httpx.MockTransport(handler))
    assert recorded_ranges(requests) == [None]
    # Pre-existing behavior for unknown size: any received byte trips
    # `completed > file.size` -> DownloadSizeError, same as before.
    assert isinstance(results[-1], Err)
    assert isinstance(results[-1].err, DownloadSizeError)
