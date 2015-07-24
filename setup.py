import os

from setuptools import setup
from setuptools import find_packages


HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(HERE, 'README.md')) as file_obj:
    README = file_obj.read()


REQUIREMENTS = [
    'httplib2 >= 0.9.1',
    'oauth2client >= 1.4.6',
    'protobuf >= 3.0.0a3',
    'six >= 1.6.1',
]

setup(
    name='gcloud-bigtable',
    version='0.0.1',
    description='API Client library for Google Cloud Bigtable',
    author='Google Cloud Platform',
    author_email='daniel.j.hermes@gmail.com',
    long_description=README,
    scripts=[],
    url='https://github.com/dhermes/gcloud-python-bigtable',
    packages=find_packages(),
    license='Apache 2.0',
    platforms='Posix; MacOS X; Windows',
    include_package_data=True,
    zip_safe=False,
    install_requires=REQUIREMENTS,
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Topic :: Internet',
    ]
)
