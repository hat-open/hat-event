import sys

from hat.event.server.backends.lmdb.convert.main import main


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-lmdb-convert'
    sys.exit(main())
