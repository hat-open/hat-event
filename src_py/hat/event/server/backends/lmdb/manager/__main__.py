import sys

from hat.event.server.backends.lmdb.manager.main import main


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-lmdb-manager'
    sys.exit(main())
