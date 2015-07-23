# Copyright 2015 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Logging settings for Google Cloud BigTable API package."""


import argparse
import logging
import os
import sys


LOGGER = logging.getLogger('gcloud_bigtable')
PARSER = argparse.ArgumentParser(
    description='Internal gcloud-python-bigtable logging parser.')
# No help since internal.
PARSER.add_argument('--log', dest='log_level')
ENV_VAR_NAME = 'GCLOUD_LOGGING_LEVEL'

# Get logging level from user input.
DEFAULT_LEVEL = logging.INFO
MAPPING = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARN': logging.WARN,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}


def get_log_level(argv):
    """Get the logging level from command line arguments.

    :type argv: list of strings
    :param argv: Command line arguments from caller.

    :rtype: integer
    :returns: The logging level parsed from the command line flags.
    """
    parsed_args, _ = PARSER.parse_known_args(argv)
    # Use upper case since that is used in MAPPING.
    log_level = parsed_args.log_level or os.getenv(ENV_VAR_NAME)
    return MAPPING.get(log_level, DEFAULT_LEVEL)


def setup_logger(logger, argv):
    """Set the logging level on a logger.

    Uses ``get_log_level`` from

    :type logger: :class:`logging.Logger`
    :param logger: The logger that needs to be set-up.

    :type argv: list of strings
    :param argv: Command line arguments from caller.
    """
    log_level = get_log_level(argv)
    logger.setLevel(log_level)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)
    logger.addHandler(stream_handler)


setup_logger(LOGGER, sys.argv)
