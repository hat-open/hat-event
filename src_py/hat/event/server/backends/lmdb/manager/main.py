import argparse
import sys

from hat import json

from hat.event.server.backends.lmdb.manager import query


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument('--indent', type=int, default=None)
    subparsers = parser.add_subparsers(dest='action', required=True)

    query.create_argument_parser(subparsers)

    return parser


def main():
    parser = create_argument_parser()
    args = parser.parse_args()

    if args.action == 'query':
        results = query.query(args)

    else:
        raise ValueError('unsupported action')

    for result in results:
        print(json.encode(result, indent=args.indent))


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-lmdb-manager'
    sys.exit(main())
