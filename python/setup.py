# Copyright 2016 Husky Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

setup(
    name='pyhusky',
    version='0.1.2',

    author='The Husky Team',
    author_email='support@husky-project.com',
    license='Apache 2.0',
    description='PyHusky is to provide high-level APIs for Husky in Python.',
    url='https://github.com/husky-team/pyhusky',
    keywords='husky pyhusky distributed system',

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    packages=[
        'pyhusky',
        'pyhusky.backend',
        'pyhusky.backend.library',
        'pyhusky.common',
        'pyhusky.frontend',
        'pyhusky.frontend.library',
    ],

    install_requires=['cloudpickle', 'msgpack-python'],

    extras_require={
    },

    package_data={
    },
)
