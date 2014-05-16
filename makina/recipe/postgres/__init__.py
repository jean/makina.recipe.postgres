# -*- coding: utf-8 -*-
# Copyright (C)2007 'jeanmichel FRANCOIS'

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; see the file COPYING. If not, write to the
# Free Software Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
"""Recipe postgres"""
import logging
import argparse
import os
import time
import subprocess
import sys
from random import choice

def psql(bin_dir, postgres_data_dir, postgres_db_name, postgres_superuser_name, postgres_port):
    """ Proxy psql, defaults to our postgres
    """
    parser = argparse.ArgumentParser(description='Wrapper for %s/psql, '
            ' see that for full details.' % bin_dir)
    parser.add_argument('--bin_dir', action='store',
            default=bin_dir,
            help='location of the postgres executables '
                 '(default: "%s")' % bin_dir)
    parser.add_argument('-d', '--dbname', action='store',
            default=postgres_db_name,
            help='database name to connect to '
                 '(default: "%s")' % postgres_db_name)
    parser.add_argument('-U', '--username', action='store', 
            default=postgres_superuser_name,
            help='database user name (default: "%s")' % postgres_superuser_name)
    parser.add_argument('-p', '--port', action='store', 
            default=postgres_port,
            help='database server port (default: "%s")' % postgres_port)

    opts, unknown = parser.parse_known_args()
    optsd = vars(opts).copy()

    cmd = [os.path.join('%(bin_dir)s' % optsd, 'psql')]
    del optsd['bin_dir']

    for k, v in optsd.items():
        cmd.extend(['--%s' % k, v])

    cmd.extend(unknown)
    subprocess.call(cmd)

def pg_ctl(bin_dir, postgres_data_dir, postgres_db_name, postgres_superuser_name, postgres_port):
    """ Proxy pg_ctl, defaults to our postgres
    """
    parser = argparse.ArgumentParser(description='Wrapper for %s/pg_ctl, '
            ' see that for full details.' % bin_dir)
    parser.add_argument('--bin_dir', action='store',
            default=bin_dir,
            help='location of the postgres executables '
                 '(default: "%s")' % bin_dir)
    parser.add_argument('-D', '--pgdata', action='store',
            default=postgres_data_dir,
            help='location of the database storage area '
                 '(default: "%s")' % postgres_data_dir)
    parser.add_argument('-o', nargs='?', metavar='OPTIONS',
            help='command line options to pass to postgres '
                 '(PostgreSQL server executable) or initdb)')
    parser.add_argument('command', nargs=1, action='store',
            help="initdb, start, stop, ... (see pg_ctl --help)")

    opts, unknown = parser.parse_known_args()
    optsd = vars(opts).copy()

    cmd = [os.path.join('%(bin_dir)s' % optsd, 'pg_ctl')]
    del optsd['bin_dir']

    passthrough = optsd.get('o')
    del optsd['o']
    if passthrough:
        cmd.extend(['-o', passthrough])

    cmd.append(optsd['command'][0])
    del optsd['command']

    for k, v in optsd.items():
        cmd.extend(['--%s' % k, v])

    cmd.extend(unknown)
    subprocess.call(cmd)

class Recipe(object):
    """This recipe is used by zc.buildout"""

    def __init__(self, buildout, name, options):
        """options:
        
          - bin : path to bin folder that contains postgres binaries
          - port : port on wich postgres is started and listen
          - initdb : specify the argument to pass to the initdb command
          - cmds : list of psql cmd to execute after all those init
        
        """
        self.buildout, self.name, self.options = buildout, name, options
        options['location'] = options['prefix'] = os.path.join(
            buildout['buildout']['parts-directory'],
            name)

    def system(self, cmd, args):
        # subprocess.check_call(cmd.split())
        sys.argv[:] = [
                os.path.join(
                    self.options.get('bin'),
                    cmd.func_name)
                    ] + args
        cmd(*[self.options.get(k) for k in ('bin', 'pgdata', 'user', 'dbname', 'port')])

    def pgdata_exists(self):
        return os.path.exists(self.options['pgdata']) 

    def install(self):
        """installer"""
        self.logger = logging.getLogger(self.name)
        if not os.path.exists(self.options['location']):
            os.mkdir(self.options['location'])
        # Don't touch an existing database
        if self.pgdata_exists():
            self.stopdb()
            return self.options['location']
        self.stopdb()
        self.initdb()
        self.startdb()
        self.do_cmds()
        self.stopdb()
        return self.options['location']

    def update(self):
        """updater"""
        self.logger = logging.getLogger(self.name)
        # TODO: only stop if we have commands
        self.stopdb()
        if not self.pgdata_exists():
            self.initdb()
        self.startdb()
        self.do_cmds()
        self.stopdb()
        return self.options['location']

    def startdb(self):
        if os.path.exists(os.path.join(self.options.get('pgdata'),'postmaster.pid')):
            self.system(pg_ctl, ['restart'])
        else:
            self.system(pg_ctl, ['start'])
        time.sleep(4)

    def stopdb(self):
        if os.path.exists(os.path.join(self.options.get('pgdata'),'postmaster.pid')):
            self.system(pg_ctl, ['stop'])
            time.sleep(4)

    def isdbstarted(self):
        PIDFILE = os.path.join(self.options.get('pgdata'),'postmaster.pid')
        return os.path.exists(pg_ctl) and os.path.exists(PIDFILE)

    def initdb(self):
        initdb_options = self.options.get('initdb',None)
        bin_dir = self.options.get('bin','')
        if initdb_options and not self.pgdata_exists():
            self.system(pg_ctl, ['initdb'] + initdb_options.split())
            # self.system('%s %s' % (os.path.join(bin_dir, 'initdb'), initdb_options) )

    def do_cmds(self):
        cmds = self.options.get('cmds', None)
        bin_dir = self.options.get('bin')
        if not cmds: return None
        cmds = cmds.split(os.linesep)
        for cmd in cmds:
            if not cmd: continue
            cmd = os.path.join([bin_dir, cmd])
            try:
                subprocess.check_call(cmd)
            except CalledProcessError, e:
                print cmd, 'failed. Return code: ', e.returncode
        dest = self.options['location']
