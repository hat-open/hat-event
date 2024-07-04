import sys

from hat.event.manager.main import main


if __name__ == '__main__':
    sys.argv[0] = 'hat-event-manager'
    sys.exit(main())
