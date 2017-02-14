#!/usr/bin/env python

import getopt
import os
import sys

pylint = os.getenv('PYLINT', 'pylint')

pyhusky_root = os.getenv('PYHUSKY_ROOT', '.')
default_root = pyhusky_root + '/python'
os.chdir(default_root)
default_module = 'pyhusky'
default_rcfile = 'pylintrc'

def usage():
    print("Run command as: $ pylint.py [$PYTHON_MODULE]")
    print("             -> $ pylint.py python/pyhusky/frontend/env.py")
    print("             -> $ pylint.py pyhusky/frontend")
    print("             -> $ pylint.py")

def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        opts, args = getopt.getopt(argv[1:], "h", ["help"])
    except getopt.GetoptError as err:
        print(err) # will print something like "option -a not recognized"
        usage()
        return 2

    for o, _ in opts:
        if o in ("-h", "--help"):
            usage()
            return 0

    module = ''
    if len(args) == 0:
        module = default_module
        cmd = '{} {} --rcfile {}'.format(pylint, default_module, default_rcfile)
        print('[Run] ' + cmd)
        os.system(cmd)
    else:
        module = args
        for m in module:
            cmd = '{} {} --rcfile {}'.format(pylint, m.replace('python/', ''), default_rcfile)
            print('[Run] ' + cmd)
            os.system(cmd)


if __name__ == '__main__':
    sys.exit(main())
