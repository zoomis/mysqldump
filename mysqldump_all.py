#!/usr/bin/env python

import time
import os
import filecmp
import csv
import re
import subprocess
import sys

# Global variables
DB_USER = 'root'
DB_PASS = 'abc123'
DB_HOST = 'localhost'
OUTPUT_DIR = 'output'
# frequency second
FREQUENCY = 5
COUNT = 2
DATA_SOURCES=["nova","keystone","neutron"]
EXCLUDED_TABLES = None
LOG_SERVER = 'localhost'
LOG_USER = 'logger'
KEY_FILE = 'logger.key'
TARGET= '~/openstack'
dump_logfile = "dump.log"
dump_log = None

# This prevents prematurely closed pipes from raising
# an exception in Python
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)


def log(msg):
    global dump_log
    print msg
    dump_log.write(msg+"\n")


def get_tables(db_name):
    """
    the given data is
    +-----
    | table_name1
    | table_name2
    +-----
    :param table_data:
    :return:
    """
    cmd = 'mysql -u %s -p%s -h%s %s -e "show tables"' % \
        (DB_USER, DB_PASS, DB_HOST, db_name)
    table_data = subprocess.check_output(cmd, shell=True)
    lines = re.split('\n', table_data)[1:-1]
    tables = []
    for table in lines:
        if 'alembic' not in table:
            tables.append(table)
    return tables

def transfer_output_to_server(server, user, key, output, target):
    os.system("scp -r -q -i %s -o StrictHostKeyChecking=no %s %s@%s:%s" %
              (key, output, user, server, target))

def is_create(line):
    """
    Returns true if the line begins a SQL create statement.
    """
    return line.startswith('CREATE TABLE') or False

def is_end_create(line):
    return line.startswith('  PRIMARY KEY') or False


def is_insert(line):
    """
    Returns true if the line begins a SQL insert statement.
    """
    return line.startswith('INSERT INTO') or False


def get_values(line):
    """
    Returns the portion of an INSERT statement containing values
    """
    return line.partition('` VALUES ')[2]


def values_sanity_check(values):
    """
    Ensures that values from the INSERT statement meet basic checks.
    """
    assert values
    assert values[0] == '('
    # Assertions have not been raised
    return True


def parse_values(values, outfile):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
    )

    writer = csv.writer(outfile, quoting=csv.QUOTE_MINIMAL)
    for reader_row in reader:
        for column in reader_row:
            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False
                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:
                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True
                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    writer.writerow(latest_row)
                    latest_row = []
                # If we're beginning a new row, eliminate the
                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]
            # Add our column to the row we're working on.
            latest_row.append(column)
        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and
        # the close paren.
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
            writer.writerow(latest_row)

def dump_table(data, output):
    """
    Dump table to CSV format
    First line is attribute list
    Values are listed from the second line
    :param data:
    :param output:
    :return:
    """
    is_created = False
    lines = re.split('\n', data)
    attributes = []
    created = False
    for line in lines:
        if created:
            if is_end_create(line):
                break
            else:
                line_split = line.split('`')
                if len(line_split) > 1:
                    attributes.append(line.split('`')[1])
        else:
            created = is_create(line)
    writer = csv.writer(output, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(attributes)
    for line in lines:
        # Look for an INSERT statement and parse it.
        if is_insert(line):
            values = get_values(line)
            if values_sanity_check(values):
                parse_values(values, output)
                is_created = True

    return is_created

def dump_datasources(prev_data_sources, table_list, excluded_tables):
    """
    Dump data sources
    Create output folder and files
    output
      - YYYYDDMM
        - HHMMSS
           {database}.log
           {database}-{table}.log
    :return:
    """
    dirname = '%s/%s-%s/'%(OUTPUT_DIR, time.strftime("%Y%m%d"), time.strftime("%H%M%S"))

    # if the log directory is not exitsted, create it first.
    d = os.path.dirname(dirname)
    if not os.path.exists(d):
        os.makedirs(d)

    # For checking update, we need to keep the previous data
    # [{'dbname': {'table_name': filename}}]

    for ds in DATA_SOURCES:
        db_name = ds
        tables = table_list[db_name]
        is_changed = False
        for table in tables:
            if table in excluded_tables[db_name]:
                log("%s table is excluded ...." % table)
                continue
            filename = '%s%s-%s.log'%(dirname, db_name, table)
            cmd = 'mysqldump -u %s -p%s -h%s %s %s' % \
                  (DB_USER, DB_PASS, DB_HOST, db_name, table)
            dump_data = subprocess.check_output(cmd, shell=True)
            output = open(filename, 'w')
            dump_table(dump_data, output)
            output.close()

            # create a file for each database table
            if not prev_data_sources[db_name][table]:
                prev_data_sources[db_name][table] = filename
                is_changed = True
                log( "%s Generated !!!" % filename)
            else:
                # compare prev_data_sources and current file using diff
                if not filecmp.cmp(prev_data_sources[db_name][table], filename):
                    prev_data_sources[db_name][table] = filename
                    is_changed = True
                    log.write( "%s Generated !!!" % filename)
                else:
                    os.remove(filename)

        #if is_changed:
            # excute mysqldump for database
            #db_filename = '%s%s.sql'%(dirname, db_name)
            #cmd = 'mysqldump -u %s -p%s -h%s %s > %s' % (DB_USER, DB_PASS, DB_HOST, db_name, db_filename)
            #dump_log.write("%s Generated !!!\n" % db_filename)
            #os.system(cmd)

    # Copy all logs to logging server
    TARGET_DIR = "%s/%s" % (TARGET, OUTPUT_DIR)
    log("Copy files to %s" % LOG_SERVER)
    transfer_output_to_server(LOG_SERVER, LOG_USER, KEY_FILE, dirname, TARGET_DIR)

def main():
    """
    Dump data sources periodically
    :return:
    """
    # if the log directory is not exitsted, create it first.
    global dump_log
    d = os.path.dirname(dump_logfile)
    if not os.path.exists(d):
        os.makedirs(d)

    transfer_output_to_server(LOG_SERVER, LOG_USER, KEY_FILE, OUTPUT_DIR, TARGET)

    excluded_tables = dict()

    for ds in DATA_SOURCES:
        excluded_tables[ds] = []

    if EXCLUDED_TABLES:
        dbt_list = EXCLUDED_TABLES.split(',')
        for dbt in dbt_list:
            t_list = dbt.split('-')
            if t_list[0] in excluded_tables.keys():
                excluded_tables[t_list[0]].append(t_list[1])

    table_list = dict()
    prev_data_sources = dict()
    for ds in DATA_SOURCES:
        tables = get_tables(ds)
        table_list[ds] = tables
        table_dict = dict()
        for table in tables:
            table_dict[table] = None
        prev_data_sources[ds] = table_dict

    for i in range(0, COUNT):

        dump_log = open(dump_logfile,'a')
        log("Trial %s/%s" % (i+1, COUNT))
        dump_datasources(prev_data_sources, table_list, excluded_tables)
        dump_log.close()

        time.sleep(FREQUENCY)

if __name__ == "__main__":
    if len(sys.argv) == 13:
        DB_USER = sys.argv[1]
        DB_PASS = sys.argv[2]
        DB_HOST = sys.argv[3]
        FREQUENCY = int(sys.argv[4])
        COUNT = int(sys.argv[5])/FREQUENCY
        OUTPUT_DIR = sys.argv[6]
        DATA_SOURCES = sys.argv[7].split(',')
        EXCLUDED_TABLES = sys.argv[8]
        LOG_SERVER = sys.argv[9]
        LOG_USER = sys.argv[10]
        KEY_FILE = sys.argv[11]
        TARGET = sys.argv[12]
        dump_logfile = "%s/dump.log" % OUTPUT_DIR
        main()
    else:
        print "Usage: mysqldump.py [DB_USER] [DB_PASS] [DB_HOST] " \
        " [FREQUENCY (sec)] [DURATION (sec)] [OUTPUT_DIR] [DATABASES]" \
        " [EXCLUDED TABLES] [LOG SERVER] [LOG USER] [KEY FILE]"
