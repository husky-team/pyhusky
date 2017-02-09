PyHusky
=======

[![Build Status](https://travis-ci.org/husky-team/pyhusky.svg?branch=master)](https://travis-ci.org/husky-team/pyhusky)
[![PyHusky License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/husky-team/pyhusky/blob/master/LICENSE)

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
    git submodule update --init --recursive

We assume the root directory of PyHusky is `$PYHUSKY_ROOT`. Go to `$PYHUSKY_ROOT` and do a out-of-source build using CMake:

    cd $PYHUSKY_ROOT
    mkdir release
    cd release
    cmake -DCMAKE_BUILD_TYPE=Release ..
    make help                 # List all build target
    make -j{N} PyHuskyMaster  # Build the Master of PyHusky
    make -j{N} PyHuskyDaemon  # Build the Daemon of PyHusky

Configuration
-------------

It is the same as configuration for Husky. See (Config-How-to)[https://github.com/husky-team/husky/wiki/Config-How-to].

Example to run:
-------------

First, run `PyHuskyMaster`:

    ./PyHuskyMaster --conf /path/to/your/conf

Second, run `PyHuskyDaemon`:

In the single-machine environment, use the following,

    ./PyHuskyDaemon --conf /path/to/your/conf

In the distributed environment, use the following to execute workers on all machines,

    ./exec.sh PyHuskyDaemon --conf /path/to/your/conf

Then, start a `python` terminal in `$PYHUSKY_ROOT`:

```python
    >>> import frontend as ph
    >>> ph.env.pyhusky_start("master", xxxxx)
    >>> words = ["hello", "world", "hello", "husky"]
    >>> word_list = ph.env.parallelize(words)
    >>> wc = word_list.map(lambda x: (x, 1)).reduce_by_key(lambda x, y: x + y).collect()
    >>> print wc
    [('hello', 2), ('husky', 1), ('world', 1)]
```

License
---------------

Copyright 2016-2017 Husky Team

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
