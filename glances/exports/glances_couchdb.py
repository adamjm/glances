# -*- coding: utf-8 -*-
#
# This file is part of Glances.
#
# Copyright (C) 2019 Nicolargo <nicolas@nicolargo.com>
#
# Glances is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Glances is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""CouchDB interface class."""

import sys
from datetime import datetime

from glances.logger import logger
from glances.exports.glances_export import GlancesExport

import couchdb
from cloudant.client import Cloudant
from cloudant.client import CouchDB

class Export(GlancesExport):

    """This class manages the CouchDB export module."""

    def __init__(self, config=None, args=None):
        """Init the CouchDB export IF."""
        super(Export, self).__init__(config=config, args=args)

        # Mandatories configuration keys (additional to host and port)
        self.db = None

        # Optionals configuration keys
        self.user = None
        self.password = None

        # Load the Couchdb configuration file section
        self.export_enable = self.load_conf('couchdb',
                                            mandatories=['host', 'port', 'db'],
                                            options=['user', 'password', 'cloudant'])
        if not self.export_enable:
            sys.exit(2)

        # Init the CouchDB client
        self.client = self.init()

    def init(self):
        """Init the connection to the CouchDB server."""
        if not self.export_enable:
            return None

        if self.type is not None and self.type:
            server_uri = 'https://{}:{}/'.format(self.host,
                                                self.port)
        else:
            server_uri = 'http://{}:{}/'.format(self.host,
                                                self.port)

        try:
            if self.type is not None and self.type:
                client = Cloudant(self.user, self.password, url=server_uri, connect=True)
            else:
                client = CouchDB(self.user, self.password, url=server_uri, connect=True)
        except Exception as e:
            logger.critical("Cannot connect to CouchDB server %s (%s)" % (server_uri, e))
            sys.exit(2)
        else:
            logger.info("Connected to the CouchDB server %s" % server_uri)

        try:
            client[self.db]
        except Exception as e:
            # Database did not exist
            # Create it...
            client.create_database(self.db)
        else:
            logger.info("There is already a %s database" % self.db)

        return client

    def database(self):
        """Return the CouchDB database object"""
        return self.client[self.db]

    def export(self, name, columns, points):
        """Write the points to the CouchDB server."""
        logger.debug("Export {} stats to CouchDB".format(name))

        # Create DB input
        data = dict(zip(columns, points))

        # Set the type to the current stat name
        data['type'] = name
        data['time'] = couchdb.mapping.DateTimeField()._to_json(datetime.now())

        # Write input to the CouchDB database
        # Result can be view: http://127.0.0.1:5984/_utils
        try:
            self.client[self.db].create_document(data)
        except Exception as e:
            logger.error("Cannot export {} stats to CouchDB ({})".format(name, e))
