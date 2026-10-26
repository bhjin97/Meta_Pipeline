class HadoopStorage:
    """Minimal Hadoop FileSystem adapter used by Phase 3 publication."""

    def __init__(self, spark, root: str):
        self._jvm = spark._jvm
        self._conf = spark._jsc.hadoopConfiguration()
        self._fs = self._jvm.org.apache.hadoop.fs.FileSystem.get(
            self._jvm.java.net.URI(root), self._conf
        )

    def _path(self, value: str):
        return self._jvm.org.apache.hadoop.fs.Path(value)

    def exists(self, path: str) -> bool:
        return self._fs.exists(self._path(path))

    def read_bytes(self, path: str) -> bytes:
        stream = self._fs.open(self._path(path))
        chunks = []
        try:
            while True:
                chunk = bytes(stream.readNBytes(1024 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
        finally:
            stream.close()
        return b"".join(chunks)

    def write_bytes(self, path: str, payload: bytes, overwrite: bool) -> None:
        stream = self._fs.create(self._path(path), overwrite)
        try:
            stream.write(bytearray(payload))
        finally:
            stream.close()

    def has_parquet_objects(self, path: str) -> bool:
        target = self._path(path)
        if not self._fs.exists(target):
            return False
        files = self._fs.listFiles(target, True)
        while files.hasNext():
            status = files.next()
            if status.isFile() and status.getPath().getName().endswith(".parquet"):
                return True
        return False

