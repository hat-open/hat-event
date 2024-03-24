from pathlib import Path
import sys
import typing

from hat import util

from hat.event.backends.lmdb.convert import convert_v06_to_v07
from hat.event.backends.lmdb.convert import convert_v07_to_v09
from hat.event.backends.lmdb.convert.version import Version, get_version


class Conversion(typing.NamedTuple):
    src_version: Version
    dst_version: Version
    convert: typing.Callable[[Path, Path], None]


target_version = Version.v09

conversions = [Conversion(src_version=Version.v06,
                          dst_version=Version.v07,
                          convert=convert_v06_to_v07.convert),
               Conversion(src_version=Version.v07,
                          dst_version=Version.v09,
                          convert=convert_v07_to_v09.convert)]


def main():
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} SRC_DB_PATH DST_DB_PATH", file=sys.stderr)
        sys.exit(1)

    src_path = Path(sys.argv[1])
    dst_path = Path(sys.argv[2])

    if not src_path.exists():
        print(f"invalid SRC_DB_PATH: {src_path}", file=sys.stderr)
        sys.exit(1)

    if dst_path.exists():
        print(f"existing DST_DB_PATH: {dst_path}", file=sys.stderr)
        sys.exit(1)

    try:
        convert(src_path, dst_path)

    except Exception as e:
        print(f"conversion error: {e}", file=sys.stderr)
        sys.exit(1)


def convert(src_path: Path,
            dst_path: Path):
    version = get_version(src_path)

    if version == target_version:
        raise Exception(f"{src_path} already up to date")

    tmp_src_path = src_path

    while version != target_version:
        conversion = util.first(conversions,
                                lambda i: i.src_version == version)
        if not conversion:
            raise Exception(f"unsupported version {version.value}")

        if conversion.dst_version == target_version:
            tmp_dst_path = dst_path

        else:
            tmp_dst_path = dst_path.with_suffix(
                f"{dst_path.suffix}.{conversion.dst_version.name}")

        if tmp_dst_path.exists():
            tmp_dst_path.unlink()

        conversion.convert(tmp_src_path, tmp_dst_path)
        version = conversion.dst_version

        if tmp_src_path != src_path:
            tmp_src_path.unlink()

        tmp_src_path = tmp_dst_path


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-lmdb-convert'
    main()
