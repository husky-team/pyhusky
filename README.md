PyHusky
=======

PyHusky is to provide high-level APIs for [Husky](https://github.com/husky-team/husky) in Python.


Dependencies
-------------

All dependencies of Husky are needed.

Build
-----

Download the source code:

    git clone https://github.com/husky-team/pyhusky.git

Download Husky source code as a submodule:

    git submodule init
    git submodule update --remote --recursive

Do a out-of-source build using CMake:

    mkdir release
    cd release
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make help                 # List all build target
    make -j{N} PyHuskyMaster  # Build the Master of PyHusky
    make -j{N} PyHuskyDaemon  # Build the Daemon of PyHusky
