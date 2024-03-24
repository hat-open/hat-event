import argparse
import sys

from hat.event.backends.lmdb.manager import copy
from hat.event.backends.lmdb.manager import query


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='action', required=True)

    query.create_argument_parser(subparsers)
    copy.create_argument_parser(subparsers)

    return parser


def main():
    parser = create_argument_parser()
    args = parser.parse_args()

    if args.action == 'query':
        query.query(args)

    elif args.action == 'copy':
        copy.copy(args)

    else:
        raise ValueError('unsupported action')


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-lmdb-manager'
    sys.exit(main())
