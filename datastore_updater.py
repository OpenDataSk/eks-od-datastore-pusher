#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2014, Adrià Mercader (https://github.com/amercader)
# Copyright (c) 2018, Peter Hanecak <hanecak@opendata.sk>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import configparser
import csv
import datetime
import json
import os
import pickle
import sys

import requests

USAGE = '''

    datastore_update.py setup
        Creates a dataset in the remote CKAN instance, adds a DataStore
        resource to it and pushes a first dump of the earthquakes that happened
        during the last day. It will return the resource id that you must
        write in your configuration file if you want to regularly update the
        DataStore table with the `update` command.

    datastore_update.py update
        Requests the last hour eartquakes from the remote server and pushes the
        records to the DataStore. You need to include the resource_id returned
        by the previous command to your configuration file before running this
        one. You should run this command periodically every each hour, eg with
        cron job.

'''

BATCH_SIZE = 10000
STATE_FILE = 'datastore_updater.state'

# state keys
STATE_LAST_PROCESSED = 'last_processed.'


class EksBaseDatastoreUpdater:
    """Base class for EKS datastore pusher containing common code and structures."""

    # basic per-dataset stuff, here set to None, proper values set in derived classes
    CONFIG_SECTION = None
    DIRECTORY_SUBDIR = None
    CSV_FN_PATTERN = None
    #  FIXME: rework 'next_csvdate()' & co. to pass timestamp so that we do not neeed this crude hack:
    CSV_FN_PATTERN_2 = None

    # description of dataset structure/schema for data in datastore
    PRIMARY_KEYS = None
    STRUCTURE = None

    # We first treat all items as 'text' (see
    # 'EksBaseDatastoreUpdater.update_month()') but then we "fix" items with
    # more precise type.
    DATE_ITEM_NAMES = None
    FLOAT_ITEM_NAMES = None
    INT_ITEM_NAMES = None


    def __init__(self):
        self.state = {}

        # items from main section, common to all EKS datasets
        config = configparser.SafeConfigParser()
        config.read('config.ini')
        for key in ('ckan_url', 'api_key',):
            if not config.has_option('main', key):
                exit('Please fill the {0} option in the main section of the config.ini file'
                     .format(key))

        self.ckan_url = config.get('main', 'ckan_url').rstrip('/')
        self.api_key = config.get('main', 'api_key')
        self.ssl_verify = config.getboolean('main', 'ssl_verify', fallback=True)

        self.directory_root = config.get('main', 'directory_root')
        if not self.directory_root:
            exit('You need to add the path to root directory with EKS files ' +
                 'to your configuration file.')

        # items from subsections, for a specific EKS dataset
        if not config.has_section(self.CONFIG_SECTION):
            exit('Please add the {0} section into the config.ini file'
                 .format(self.CONFIG_SECTION))
        for key in ('dataset.name', 'dataset.title', 'dataset.notes', 'resource.id', 'resource.name', 'resource.notes'):
            if not config.has_option(self.CONFIG_SECTION, key):
                exit('Please fill the {0} option in the {1} section of the config.ini file'
                     .format(key, self.CONFIG_SECTION))

        self.dataset_name = config.get(self.CONFIG_SECTION, 'dataset.name')
        self.dataset_title = config.get(self.CONFIG_SECTION, 'dataset.title')
        self.dataset_notes = config.get(self.CONFIG_SECTION, 'dataset.notes')
        self.dataset_owner = config.get(self.CONFIG_SECTION, 'dataset.owner', fallback=None)
        self.resource_id = config.get(self.CONFIG_SECTION, 'resource.id')
        self.resource_name = config.get(self.CONFIG_SECTION, 'resource.name')
        self.resource_notes = config.get(self.CONFIG_SECTION, 'resource.notes')


    def load_state(self):
        if not os.path.isfile(STATE_FILE):
            print('info: no previous state found (%s)' % STATE_FILE)
            return

        state_file = open(STATE_FILE, "rb");
        self.state = pickle.load(state_file);
        state_file.close()


    def save_state(self):
        state_file = open(STATE_FILE, "wb");
        pickle.dump(self.state, state_file);
        state_file.close()


    def exit(self, msg=USAGE):
        print(msg)
        sys.exit(1)


    def setup(self):
        """Basic setup operation called from command line."""

        # Create a dataset first
        data = {
            'name': self.dataset_name,
            'title': self.dataset_title,
            'notes': self.dataset_notes
        }
        if not self.dataset_owner is None:
            data['owner_org'] = self.dataset_owner

        response = requests.post(
            '{0}/api/action/package_create'.format(self.ckan_url),
            data=json.dumps(data),
            headers={'Content-type': 'application/json',
                     'Authorization': self.api_key},
            verify=self.ssl_verify)

        if response.status_code != 200:
            exit('Error creating dataset: {0}'.format(response.content))

        dataset_id = response.json()['result']['id']

        # Then create a resource, empty at the beginning
        records = []

        # Manually set the field types to ensure they are handled properly
        fields = []
        for item in self.STRUCTURE:
            field = {
                'id': item['id'],
                'type': item['type']
            }
            fields.append(field)

        # Push the records to the DataStore table. This will create a resource
        # of type datastore.
        data = {
            'resource': {
                'package_id': dataset_id,
                'name': self.resource_name,
                'format': 'csv',
                'notes': self.resource_notes
            },
            'records': records,
            'fields': fields,
            'primary_key': self.PRIMARY_KEYS,
        }

        response = requests.post(
            '{0}/api/action/datastore_create'.format(self.ckan_url),
            data=json.dumps(data),
            headers={'Content-type': 'application/json',
                     'Authorization': self.api_key},
            verify=self.ssl_verify)

        if response.status_code != 200:
            exit('Error: {0}'.format(response.content))

        resource_id = response.json()['result']['resource_id']
        print('''
Dataset and DataStore resource successfully created with {0} records.
Please add the resource id to your ini file:

[{2}]
resource_id={1}
        '''.format(len(records), resource_id, self.CONFIG_SECTION))


    def find_oldest_csvdate(self):
        """Find oldest CSV file in the given directory.

        "Oldest" does not mean selection based on file modification time but instead
        based on year and month embedded in the file names.

        For "Zakazky", following file naming is used:

            ZoznamZakaziekReport_2018-3_.csv
            ZoznamZakaziekReport_2018-4_.csv

        So here, '2018-3' (a.k.a. "CVS date") would be returned."""

        # list the self.directory_root + DIRECTORY_SUBDIR, skip directories, parse out
        # available dates (YYYY-M) from file names
        csv_dir = os.path.join(self.directory_root, self.DIRECTORY_SUBDIR)
        file_dates = []
        for diritem in os.listdir(csv_dir):
            if not os.path.isfile(os.path.join(csv_dir, diritem)):
                continue
            try:
                zdate = datetime.datetime.strptime(diritem, self.CSV_FN_PATTERN)
                file_dates.append(zdate)
            except ValueError:
                print("debug: file %s does not match, skipping" % diritem)

        file_dates.sort()
        return '{d.year}-{d.month}'.format(d = file_dates[0])


    @staticmethod
    def next_csvdate(csvdate):
        """Determine next CSV date.

        Here, we simpe do "+1 month", assuming we do not have gaps in our
        CSV copy and if file is not founf, then "we went too far into
        future, no file available yet".

        Example:

            '2018-3' -> '2018-4'
        """

        zdate = datetime.datetime.strptime(csvdate, '%Y-%m')
        ztimedelta = datetime.timedelta(days=31)
        return '{d.year}-{d.month}'.format(d = (zdate + ztimedelta))


    def csv_header_check(self, row):
        """Validate a header of CSV file, i.e. fail-safe check which should prevent
        the script from loading improper data into datastore.

        Checks should be sufficient to catch at least:
        1) wrong CSV file (i.e. something completely unrelated)
        2) CSV with new/changed structure (EKS may change stuff)"""

        # Length is -1 because EKS puts separator ',' at the end which creates
        # one more empty column.
        hlen = len(row) - 1
        if hlen != len(self.STRUCTURE):
            print('error: %d items in header found, %d expected' % (hlen, len(self.STRUCTURE)))
            return False

        for sitem in self.STRUCTURE:
            # We strip \ufeff because it's in the CSV file header, thus
            # "damaging" name of the first column.
            ritem = row[sitem['csvindex']].strip('"\ufeff')
            if sitem['id'] != ritem:
                print("error: '%s' expected in row %d, '%s' found"
                    % (sitem['id'], sitem['csvindex'], ritem))
                return False

        return True


    @staticmethod
    def convert_date(eks_date):
        """Convert date used by EKS to ISO date, e.g.:
            '5.3.2018 9:00:00' -> '2018-03-05T09:00:00'
        """

        if len(eks_date) <= 0:
            return None

        date = datetime.datetime.strptime(eks_date, '%d.%m.%Y %H:%M:%S')
        # FIXME: While we are at it, we may add proper time zone (EKS is
        # pressumably using "Europe/Bratislava") so as to have proper
        # timestamps.
        return date.isoformat()


    @staticmethod
    def convert_float(eks_float):
        """Convert floats used by EKS (i.e.  decinal separated with ',') into
        proper JSON floats, e.g.:
            '1,0000' -> 1.0
        """

        if len(eks_float) <= 0:
            return None

        return float(eks_float.replace(',', '.'))


    @staticmethod
    def convert_int(eks_int):
        """We're getting "strings" from CSV, so strip quotes to get int
        suitable for JSON, e.g.:
            '504' -> 504
        """

        if len(eks_int) <= 0:
            return None

        return eks_int.strip("'")


    def upsert(self, records):
        """Upsert given records into data store."""

        if len(records) == 0:
            return

        # Push the records to the DataStore table
        data = {
            'resource_id': self.resource_id,
            'method': 'upsert',
            'records': records,
        }

        response = requests.post(
            '{0}/api/action/datastore_upsert'.format(self.ckan_url),
            data=json.dumps(data),
            headers={'Content-type': 'application/json',
                     'Authorization': self.api_key},
            verify=self.ssl_verify)

        if response.status_code != 200:
            exit('Error: {0}'.format(response.content))

        print('debug: pushed %d items in a batch' % len(records))


    def update_month(self, csvdate):
        """
        Basic update operation for one month (i.e. one CSV file).

        csvdate: portion of CSV file name with year andf month (e.g. '2018-3')

        Returns:
        - True: file processed (and we may attempt file for next month)
        - False: file not found (and thus it looks like we're done)
        """

        # prepare mapping from structure and some helpers
        # TODO: This is "static", i.e.  move it somewhere so that it runs
        # only once, not "once for each CSV file".
        mapping = {}
        for item in self.STRUCTURE:
            mapping[item['id']] = item['csvindex']

        # some other hacks:
        # - some EKS items aer too big, triggering "csv.Error: field larger than field limit"
        csv.field_size_limit(262144)

        # records to be inserted
        records = []

        # Load the CSV file
        csvfn = os.path.join(self.directory_root, self.DIRECTORY_SUBDIR,
            self.CSV_FN_PATTERN_2 % csvdate)
        if not os.path.exists(csvfn):
            print("file %s not available, it looks like we are done" % csvfn)
            return False

        with open(csvfn, 'r') as csvfile:
            print("loading %s ..." % csvfn)
            itemreader = csv.reader(csvfile)
            counter = 0
            for row in itemreader:
                counter += 1

                if counter == 1:
                    if not self.csv_header_check(row):
                        exit('%s header check failed' % csvfn)
                    continue

                # convert row from CSV into JSON row
                rowjson = {}
                for mitem in mapping:
                    rowjson[mitem] = row[mapping[mitem]]
                # fix dates, floats, etc.:
                for mitem in self.DATE_ITEM_NAMES:
                    rowjson[mitem] = self.convert_date(row[mapping[mitem]])
                for mitem in self.FLOAT_ITEM_NAMES:
                    rowjson[mitem] = self.convert_float(row[mapping[mitem]])
                for mitem in self.INT_ITEM_NAMES:
                    rowjson[mitem] = self.convert_int(row[mapping[mitem]])

                # TODO: add duplicate detection: For example
                # ZoznamZakaziekReport_2018-3_.csv contains 'Z20187264' at least
                # three time.  We push all accurences to 'records' here but
                # DataStore (based on IdentifikatorZakazky labeled as 'id' and
                # with 'upsert') overwrites first occurence with seconds, etc.
                # so at the end only last item gets actually stored.
                # It not clear what to do with that but at least we should
                # detect duplicates and reports their line numbers in a
                # dedicated "problems" column?

                # TODO: use the ID to obtain the row also from CKAN, so that we
                # can properly create "created" and "modified" timestamps

                records.append(rowjson)

                # batching, to avoid pushing too much in one call
                if len(records) >= BATCH_SIZE:
                    self.upsert(records)
                    records = []


        # upsert remaining records, mark state
        self.upsert(records)
        self.state[STATE_LAST_PROCESSED + self.CONFIG_SECTION] = csvdate
        self.save_state()

        print("DataStore resource '{1}' successfully updated with {0} records.".format(
            (counter - 1), self.CONFIG_SECTION))

        return True


    def update(self):
        """Basic update operation called from command line."""

        # Load "state" (YYYY-M of last processed file); if not then
        self.load_state()
        month_to_process = None
        state_key = STATE_LAST_PROCESSED + self.CONFIG_SECTION
        if state_key in self.state:
            month_to_process = self.state[state_key]
        if month_to_process is None:
            month_to_process = self.find_oldest_csvdate()

        # process "last processed" month assuming:
        # 1) if it is still "current month": we will process all, pick
        # updates, re-process again items/lines maybe needlessly (but such
        # waste is considered OK while it helps avoid more code)
        # 2) if it is "last month": we will process it "for the last time",
        # picking up latest updates and then proceed to the next (i.e.
        # current) month
        counter = 0
        while self.update_month(month_to_process):
            counter += 1
            # OK, get the name for "next month" and try it ...
            month_to_process = self.next_csvdate(month_to_process)

        print('%d files processed.' % counter)

        return


class AukcnePonuky(EksBaseDatastoreUpdater):
    """Specifics for EKS Aukcne Ponuky"""

    CONFIG_SECTION = 'aukcne_ponuky'
    DIRECTORY_SUBDIR = 'aukcne_ponuky'
    CSV_FN_PATTERN = 'ZoznamAukcnychPonukReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamAukcnychPonukReport_%s_.csv'

    PRIMARY_KEYS = ['VerejnyIdentifikatorZakazky', 'DatumPredlozeniaPonuky']
    STRUCTURE = [
        {'id': 'VerejnyIdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'DatumPredlozeniaPonuky',
            'type': 'timestamp',
            'csvindex': 1},
        {'id': 'Dodavatel_ICO',
            'type': 'text',
            'csvindex': 2},
        {'id': 'Dodavatel_ObchodnyNazov',
            'type': 'text',
            'csvindex': 3},
        {'id': 'Dodavatel_StatSidla',
            'type': 'text',
            'csvindex': 4},
        {'id': 'PredlozenaCenaBezDPH',
            'type': 'float',
            'csvindex': 5},
        {'id': 'SadzbaDPH',
            'type': 'float',
            'csvindex': 6},
        {'id': 'PredlozenaCenaSDPH',
            'type': 'float',
            'csvindex': 7},
        {'id': 'VyslednePoradie',
            'type': 'integer',
            'csvindex': 8},
        {'id': 'VstupnaPonuka',
            'type': 'bool',
            'csvindex': 9},
        {'id': 'Platna',
            'type': 'bool',
            'csvindex': 10},
    ]

    DATE_ITEM_NAMES = ['DatumPredlozeniaPonuky']
    FLOAT_ITEM_NAMES = ['PredlozenaCenaBezDPH', 'SadzbaDPH', 'PredlozenaCenaSDPH']
    INT_ITEM_NAMES = ['VyslednePoradie']


class KontrakracnePonuky(EksBaseDatastoreUpdater):
    """Specifics for EKS Kontraktacne Ponuky"""

    CONFIG_SECTION = 'kontraktacne_ponuky'
    DIRECTORY_SUBDIR = 'kontraktacne_ponuky'
    CSV_FN_PATTERN = 'ZoznamKontraktacnychPonukReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamKontraktacnychPonukReport_%s_.csv'

    PRIMARY_KEYS = ['VerejnyIdentifikatorZakazky', 'DatumPredlozeniaPonuky']
    STRUCTURE = [
        {'id': 'VerejnyIdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'DatumPredlozeniaPonuky',
            'type': 'timestamp',
            'csvindex': 1},
        {'id': 'Dodavatel_ICO',
            'type': 'text',
            'csvindex': 2},
        {'id': 'Dodavatel_ObchodnyNazov',
            'type': 'text',
            'csvindex': 3},
        {'id': 'Dodavatel_StatSidla',
            'type': 'text',
            'csvindex': 4},
        {'id': 'PredlozenaCenaBezDPH',
            'type': 'float',
            'csvindex': 5},
        {'id': 'SadzbaDPH',
            'type': 'float',
            'csvindex': 6},
        {'id': 'PredlozenaCenaSDPH',
            'type': 'float',
            'csvindex': 7},
        {'id': 'VyslednePoradie',
            'type': 'integer',
            'csvindex': 8},
        {'id': 'StavKontraktacnejPonuky',
            'type': 'text',
            'csvindex': 9},
    ]

    DATE_ITEM_NAMES = ['DatumPredlozeniaPonuky']
    FLOAT_ITEM_NAMES = ['PredlozenaCenaBezDPH', 'SadzbaDPH', 'PredlozenaCenaSDPH']
    INT_ITEM_NAMES = ['VyslednePoradie']


class OpisneFormulare(EksBaseDatastoreUpdater):
    """Specifics for EKS Opisne Formulare"""

    CONFIG_SECTION = 'opisne_formulare'
    DIRECTORY_SUBDIR = 'opisne_formulare'
    CSV_FN_PATTERN = 'ZoznamOpisnychFormularovReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamOpisnychFormularovReport_%s_.csv'

    PRIMARY_KEYS = ['OpisnyFormularIdentifikator']
    STRUCTURE = [
        {'id': 'OpisnyFormularIdentifikator',
            'type': 'text',
            'csvindex': 0},
        {'id': 'OpisnyFormularStav',
            'type': 'text',
            'csvindex': 1},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 2},
        {'id': 'OpisnyFormularKlucoveSlova',
            'type': 'text',
            'csvindex': 3},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 4},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 5},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 6},
        {'id': 'OpisnyFormularUrl',
            'type': 'text',
            'csvindex': 7},
        {'id': 'OpisnyFormularPocetVyhlasenychZakaziek',
            'type': 'integer',
            'csvindex': 8},
        {'id': 'OpisnyFormularObchodovanaSumaBezDPH',
            'type': 'float',
            'csvindex': 9},
    ]

    DATE_ITEM_NAMES = []
    FLOAT_ITEM_NAMES = ['OpisnyFormularObchodovanaSumaBezDPH']
    INT_ITEM_NAMES = ['OpisnyFormularPocetVyhlasenychZakaziek']


class Referencie(EksBaseDatastoreUpdater):
    """Specifics for EKS Referencie"""

    CONFIG_SECTION = 'referencie'
    DIRECTORY_SUBDIR = 'referencie'
    CSV_FN_PATTERN = 'ZoznamReferenciiReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamReferenciiReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'ZakazkaUrl',
            'type': 'text',
            'csvindex': 1},
        {'id': 'ReferenciaTyp',
            'type': 'text',
            'csvindex': 2},
        {'id': 'ReferenciaDatumPoslednejZmeny',
            'type': 'timestamp',
            'csvindex': 3},
        {'id': 'ReferenciaDovodPoslednejZmeny',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ReferenciaPolozka',
            'type': 'text',
            'csvindex': 5},
        {'id': 'DodavatelObchodneMeno',
            'type': 'text',
            'csvindex': 6},
        {'id': 'DodavatelICO',
            'type': 'text',
            'csvindex': 7},
        {'id': 'DodavatelStat',
            'type': 'text',
            'csvindex': 8},
    ]

    DATE_ITEM_NAMES = ['ReferenciaDatumPoslednejZmeny']
    FLOAT_ITEM_NAMES = []
    INT_ITEM_NAMES = []


class Zakazky(EksBaseDatastoreUpdater):
    """Specifics for EKS Zazkazky"""

    CONFIG_SECTION = 'zakazky'
    DIRECTORY_SUBDIR = 'zakazky'
    CSV_FN_PATTERN = 'ZoznamZakaziekReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamZakaziekReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'ZakazkaUrl',
            'type': 'text',
            'csvindex': 1},
        {'id': 'StavZakazky',
            'type': 'text',
            'csvindex': 2},
        {'id': 'PouzityPostup',
            'type': 'text',
            'csvindex': 3},
        {'id': 'ObjednavatelDruh',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ObjednavatelObchodneMeno',
            'type': 'text',
            'csvindex': 5},
        {'id': 'ObjednavatelICO',
            'type': 'text',
            'csvindex': 6},
        {'id': 'ObjednavatelStat',
            'type': 'text',
            'csvindex': 7},
        {'id': 'ObjednavatelObec',
            'type': 'text',
            'csvindex': 8},
        {'id': 'ObjednavatelPSC',
            'type': 'text',
            'csvindex': 9},
        {'id': 'ObjednavatelUlica',
            'type': 'text',
            'csvindex': 10},
        {'id': 'DatumVyhlasenia',
            'type': 'timestamp',
            'csvindex': 11},
        {'id': 'DatumZazmluvnenia',
            'type': 'timestamp',
            'csvindex': 12},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 13},
        {'id': 'OpisnyFormularKlucoveSlova',
            'type': 'text',
            'csvindex': 14},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 15},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 16},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 17},
        {'id': 'OpisnyFormularFunkcnaSpecifikacia',
            'type': 'text',
            'csvindex': 18},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaTextova',
            'type': 'text',
            'csvindex': 19},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaCiselna',
            'type': 'text',
            'csvindex': 20},
        {'id': 'MiestoPlneniaStat',
            'type': 'text',
            'csvindex': 21},
        {'id': 'MiestoPlneniaKraj',
            'type': 'text',
            'csvindex': 22},
        {'id': 'MiestoPlneniaOkres',
            'type': 'text',
            'csvindex': 23},
        {'id': 'MiestoPlneniaObec',
            'type': 'text',
            'csvindex': 24},
        {'id': 'MiestoPlneniaUlica',
            'type': 'text',
            'csvindex': 25},
        {'id': 'LehotaPlneniaOd',
            'type': 'timestamp',
            'csvindex': 26},
        {'id': 'LehotaPlneniaDo',
            'type': 'timestamp',
            'csvindex': 27},
        {'id': 'LehotaPlneniaPresne',
            'type': 'timestamp',
            'csvindex': 28},
        {'id': 'MnozstvoJednotka',
            'type': 'text',
            'csvindex': 29},
        {'id': 'MnozstvoHodnota',
            'type': 'float',
            'csvindex': 30},
        {'id': 'MaximalnaVyskaZdrojov',
            'type': 'float',
            'csvindex': 31},
        {'id': 'ZmluvnyVztah',
            'type': 'text',
            'csvindex': 32},
        {'id': 'FinancovanieEU',
            'type': 'bool',
            'csvindex': 33},
        {'id': 'HodnotiaceKriterium',
            'type': 'text',
            'csvindex': 34},
        {'id': 'LehotaNaPredkladaniePonuk',
            'type': 'timestamp',
            'csvindex': 35},
        {'id': 'PocetNotifikovanychDodavatelov',
            'type': 'integer',
            'csvindex': 36},
        {'id': 'VstupnaCena',
            'type': 'float',
            'csvindex': 37},
        {'id': 'PocetSutaziacich',
            'type': 'integer',
            'csvindex': 38},
        {'id': 'PocetPredlozenychPonuk',
            'type': 'integer',
            'csvindex': 39},
        {'id': 'ZaciatokAukcie',
            'type': 'timestamp',
            'csvindex': 40},
        {'id': 'TrvanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 41},
        {'id': 'PredlzovanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 42},
        {'id': 'ProtokolOPriebehuZadavaniaZakazky',
            'type': 'text',
            'csvindex': 43},
        {'id': 'Priloha_c1_ZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 44},
        {'id': 'Priloha_c2_VyslednePoradieDodavatelov',
            'type': 'text',
            'csvindex': 45},
        {'id': 'Priloha_c3_Zmluva',
            'type': 'text',
            'csvindex': 46},
        {'id': 'Priloha_c4A_ZaznamOSystemovychUdalostiachZakazky',
            'type': 'text',
            'csvindex': 47},
        {'id': 'Priloha_c4B_ZaznamOSystemovychUdalostiachElektronickejAukcie',
            'type': 'text',
            'csvindex': 48},
        {'id': 'AnonymnyZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 49},
        {'id': 'ObjednavkovyFormularZakazky',
            'type': 'text',
            'csvindex': 50},
    ]

    DATE_ITEM_NAMES = ['DatumVyhlasenia', 'DatumZazmluvnenia', 'LehotaPlneniaOd',
        'LehotaPlneniaDo', 'LehotaPlneniaPresne', 'LehotaNaPredkladaniePonuk',
        'ZaciatokAukcie']
    FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'MaximalnaVyskaZdrojov', 'VstupnaCena']
    INT_ITEM_NAMES = ['PocetNotifikovanychDodavatelov', 'PocetSutaziacich',
        'PocetPredlozenychPonuk', 'TrvanieAukcie_Minut', 'PredlzovanieAukcie_Minut']


class ZakazkyAZmluvyOld(EksBaseDatastoreUpdater):
    """Specifics for EKS Zazkazky a Zmluvy

    Old version of class valid for up to 2019-9."""

    CONFIG_SECTION = 'zakazky_a_zmluvy'
    DIRECTORY_SUBDIR = 'zakazky_a_zmluvy'
    CSV_FN_PATTERN = 'ZoznamZakazkyZmluvyReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamZakazkyZmluvyReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'ZakazkaUrl',
            'type': 'text',
            'csvindex': 1},
        {'id': 'StavZakazky',
            'type': 'text',
            'csvindex': 2},
        {'id': 'PouzityPostup',
            'type': 'text',
            'csvindex': 3},
        {'id': 'ObjednavatelDruh',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ObjednavatelObchodneMeno',
            'type': 'text',
            'csvindex': 5},
        {'id': 'ObjednavatelICO',
            'type': 'text',
            'csvindex': 6},
        {'id': 'ObjednavatelStat',
            'type': 'text',
            'csvindex': 7},
        {'id': 'ObjednavatelObec',
            'type': 'text',
            'csvindex': 8},
        {'id': 'ObjednavatelPSC',
            'type': 'text',
            'csvindex': 9},
        {'id': 'ObjednavatelUlica',
            'type': 'text',
            'csvindex': 10},
        {'id': 'DatumVyhlasenia',
            'type': 'timestamp',
            'csvindex': 11},
        {'id': 'DatumZazmluvnenia',
            'type': 'timestamp',
            'csvindex': 12},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 13},
        {'id': 'OpisnyFormularKlucoveSlova',
            'type': 'text',
            'csvindex': 14},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 15},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 16},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 17},
        {'id': 'OpisnyFormularFunkcnaSpecifikacia',
            'type': 'text',
            'csvindex': 18},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaTextova',
            'type': 'text',
            'csvindex': 19},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaCiselna',
            'type': 'text',
            'csvindex': 20},
        {'id': 'MiestoPlneniaStat',
            'type': 'text',
            'csvindex': 21},
        {'id': 'MiestoPlneniaKraj',
            'type': 'text',
            'csvindex': 22},
        {'id': 'MiestoPlneniaOkres',
            'type': 'text',
            'csvindex': 23},
        {'id': 'MiestoPlneniaObec',
            'type': 'text',
            'csvindex': 24},
        {'id': 'MiestoPlneniaUlica',
            'type': 'text',
            'csvindex': 25},
        {'id': 'LehotaPlneniaOd',
            'type': 'timestamp',
            'csvindex': 26},
        {'id': 'LehotaPlneniaDo',
            'type': 'timestamp',
            'csvindex': 27},
        {'id': 'LehotaPlneniaPresne',
            'type': 'timestamp',
            'csvindex': 28},
        {'id': 'MnozstvoJednotka',
            'type': 'text',
            'csvindex': 29},
        {'id': 'MnozstvoHodnota',
            'type': 'float',
            'csvindex': 30},
        {'id': 'MaximalnaVyskaZdrojov',
            'type': 'float',
            'csvindex': 31},
        {'id': 'ZmluvnyVztah',
            'type': 'text',
            'csvindex': 32},
        {'id': 'FinancovanieEU',
            'type': 'bool',
            'csvindex': 33},
        {'id': 'HodnotiaceKriterium',
            'type': 'text',
            'csvindex': 34},
        {'id': 'LehotaNaPredkladaniePonuk',
            'type': 'timestamp',
            'csvindex': 35},
        {'id': 'PocetNotifikovanychDodavatelov',
            'type': 'integer',
            'csvindex': 36},
        {'id': 'VstupnaCena',
            'type': 'float',
            'csvindex': 37},
        {'id': 'CenaBezDPH',
            'type': 'float',
            'csvindex': 38},
        {'id': 'CenaSadzbaDPH',
            'type': 'float',
            'csvindex': 39},
        {'id': 'CenaVrataneDPH',
            'type': 'float',
            'csvindex': 40},
        {'id': 'Uspora',
            'type': 'float',
            'csvindex': 41},
        {'id': 'PocetSutaziacich',
            'type': 'integer',
            'csvindex': 42},
        {'id': 'PocetPredlozenychPonuk',
            'type': 'integer',
            'csvindex': 43},
        {'id': 'ZaciatokAukcie',
            'type': 'timestamp',
            'csvindex': 44},
        {'id': 'TrvanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 45},
        {'id': 'PredlzovanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 46},
        {'id': 'ProtokolOPriebehuZadavaniaZakazky',
            'type': 'text',
            'csvindex': 47},
        {'id': 'Priloha_c1_ZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 48},
        {'id': 'Priloha_c2_VyslednePoradieDodavatelov',
            'type': 'text',
            'csvindex': 49},
        {'id': 'Priloha_c3_Zmluva',
            'type': 'text',
            'csvindex': 50},
        {'id': 'Priloha_c4A_ZaznamOSystemovychUdalostiachZakazky',
            'type': 'text',
            'csvindex': 51},
        {'id': 'Priloha_c4B_ZaznamOSystemovychUdalostiachElektronickejAukcie',
            'type': 'text',
            'csvindex': 52},
        {'id': 'AnonymnyZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 53},
        {'id': 'ObjednavkovyFormularZakazky',
            'type': 'text',
            'csvindex': 54},
        {'id': 'DodavatelICO',
            'type': 'text',
            'csvindex': 55},
        {'id': 'DodavatelStat',
            'type': 'text',
            'csvindex': 56},
        {'id': 'DodavatelKraj',
            'type': 'text',
            'csvindex': 57},
        {'id': 'DodavatelOkres',
            'type': 'text',
            'csvindex': 58},
        {'id': 'DodavatelObec',
            'type': 'text',
            'csvindex': 59},
        {'id': 'DodavatelPSC',
            'type': 'text',
            'csvindex': 60},
        {'id': 'DodavatelUlica',
            'type': 'text',
            'csvindex': 61},
        {'id': 'ReferenciaTyp',
            'type': 'text',
            'csvindex': 62},
        {'id': 'ReferenciaDatumPoslednejZmeny',
            'type': 'text',
            'csvindex': 63},
        {'id': 'ReferenciaDovodPoslednejZmeny',
            'type': 'text',
            'csvindex': 64},
        {'id': 'ReferenciaPolozka',
            'type': 'text',
            'csvindex': 65},
    ]

    DATE_ITEM_NAMES = ['DatumVyhlasenia', 'DatumZazmluvnenia', 'LehotaPlneniaOd',
        'LehotaPlneniaDo', 'LehotaPlneniaPresne', 'LehotaNaPredkladaniePonuk',
        'ZaciatokAukcie']
    FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'MaximalnaVyskaZdrojov', 'VstupnaCena',
        'CenaBezDPH', 'CenaSadzbaDPH', 'CenaVrataneDPH', 'Uspora']
    INT_ITEM_NAMES = ['PocetNotifikovanychDodavatelov', 'PocetSutaziacich',
        'PocetPredlozenychPonuk', 'TrvanieAukcie_Minut', 'PredlzovanieAukcie_Minut']


class ZakazkyAZmluvy(EksBaseDatastoreUpdater):
    """Specifics for EKS Zazkazky a Zmluvy"""

    CONFIG_SECTION = 'zakazky_a_zmluvy'
    DIRECTORY_SUBDIR = 'zakazky_a_zmluvy'
    CSV_FN_PATTERN = 'ZoznamZakazkyZmluvyReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamZakazkyZmluvyReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'ZakazkaUrl',
            'type': 'text',
            'csvindex': 1},
        {'id': 'StavZakazky',
            'type': 'text',
            'csvindex': 2},
        {'id': 'PouzityPostup',
            'type': 'text',
            'csvindex': 3},
        {'id': 'ObjednavatelDruh',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ObjednavatelObchodneMeno',
            'type': 'text',
            'csvindex': 5},
        {'id': 'ObjednavatelICO',
            'type': 'text',
            'csvindex': 6},
        {'id': 'ObjednavatelStat',
            'type': 'text',
            'csvindex': 7},
        {'id': 'ObjednavatelObec',
            'type': 'text',
            'csvindex': 8},
        {'id': 'ObjednavatelPSC',
            'type': 'text',
            'csvindex': 9},
        {'id': 'ObjednavatelUlica',
            'type': 'text',
            'csvindex': 10},
        {'id': 'DatumVyhlasenia',
            'type': 'timestamp',
            'csvindex': 11},
        {'id': 'DatumZazmluvnenia',
            'type': 'timestamp',
            'csvindex': 12},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 13},
        {'id': 'OpisnyFormularKlucoveSlova',
            'type': 'text',
            'csvindex': 14},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 15},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 16},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 17},
        {'id': 'OpisnyFormularFunkcnaSpecifikacia',
            'type': 'text',
            'csvindex': 18},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaTextova',
            'type': 'text',
            'csvindex': 19},
        {'id': 'OpisnyFormularTechnickaSpecifikaciaCiselna',
            'type': 'text',
            'csvindex': 20},
        {'id': 'MiestoPlneniaStat',
            'type': 'text',
            'csvindex': 21},
        {'id': 'MiestoPlneniaKraj',
            'type': 'text',
            'csvindex': 22},
        {'id': 'MiestoPlneniaOkres',
            'type': 'text',
            'csvindex': 23},
        {'id': 'MiestoPlneniaObec',
            'type': 'text',
            'csvindex': 24},
        {'id': 'MiestoPlneniaUlica',
            'type': 'text',
            'csvindex': 25},
        {'id': 'LehotaPlneniaOd',
            'type': 'timestamp',
            'csvindex': 26},
        {'id': 'LehotaPlneniaDo',
            'type': 'timestamp',
            'csvindex': 27},
        {'id': 'LehotaPlneniaPresne',
            'type': 'timestamp',
            'csvindex': 28},
        {'id': 'MnozstvoJednotka',
            'type': 'text',
            'csvindex': 29},
        {'id': 'MnozstvoHodnota',
            'type': 'float',
            'csvindex': 30},
        {'id': 'MaximalnaVyskaZdrojov',
            'type': 'float',
            'csvindex': 31},
        {'id': 'ZmluvnyVztah',
            'type': 'text',
            'csvindex': 32},
        {'id': 'FinancovanieEU',
            'type': 'bool',
            'csvindex': 33},
        {'id': 'HodnotiaceKriterium',
            'type': 'text',
            'csvindex': 34},
        {'id': 'LehotaNaPredkladaniePonuk',
            'type': 'timestamp',
            'csvindex': 35},
        {'id': 'PocetNotifikovanychDodavatelov',
            'type': 'integer',
            'csvindex': 36},
        {'id': 'VstupnaCena',
            'type': 'float',
            'csvindex': 37},
        {'id': 'CenaBezDPH',
            'type': 'float',
            'csvindex': 38},
        {'id': 'CenaSadzbaDPH',
            'type': 'float',
            'csvindex': 39},
        {'id': 'CenaVrataneDPH',
            'type': 'float',
            'csvindex': 40},
        {'id': 'Uspora',
            'type': 'float',
            'csvindex': 41},
        {'id': 'PocetSutaziacich',
            'type': 'integer',
            'csvindex': 42},
        {'id': 'PocetPredlozenychPonuk',
            'type': 'integer',
            'csvindex': 43},
        {'id': 'ZaciatokAukcie',
            'type': 'timestamp',
            'csvindex': 44},
        {'id': 'TrvanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 45},
        {'id': 'PredlzovanieAukcie_Minut',
            'type': 'integer',
            'csvindex': 46},
        {'id': 'ProtokolOPriebehuZadavaniaZakazky',
            'type': 'text',
            'csvindex': 47},
        {'id': 'Priloha_c1_ZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 48},
        {'id': 'Priloha_c2_VyslednePoradieDodavatelov',
            'type': 'text',
            'csvindex': 49},
        {'id': 'Priloha_c3_Zmluva',
            'type': 'text',
            'csvindex': 50},
        {'id': 'Priloha_c4A_ZaznamOSystemovychUdalostiachZakazky',
            'type': 'text',
            'csvindex': 51},
        {'id': 'Priloha_c4B_ZaznamOSystemovychUdalostiachElektronickejAukcie',
            'type': 'text',
            'csvindex': 52},
        {'id': 'AnonymnyZmluvnyFormularZakazky',
            'type': 'text',
            'csvindex': 53},
        {'id': 'ObjednavkovyFormularZakazky',
            'type': 'text',
            'csvindex': 54},
        {'id': 'DodavatelICO',
            'type': 'text',
            'csvindex': 55},
        {'id': 'DodavatelStat',
            'type': 'text',
            'csvindex': 56},
        {'id': 'DodavatelKraj',
            'type': 'text',
            'csvindex': 57},
        {'id': 'DodavatelOkres',
            'type': 'text',
            'csvindex': 58},
        {'id': 'DodavatelObec',
            'type': 'text',
            'csvindex': 59},
        {'id': 'DodavatelPSC',
            'type': 'text',
            'csvindex': 60},
        {'id': 'DodavatelUlica',
            'type': 'text',
            'csvindex': 61},
        {'id': 'ReferenciaTyp',
            'type': 'text',
            'csvindex': 62},
        {'id': 'ReferenciaDatumPoslednejZmeny',
            'type': 'text',
            'csvindex': 63},
        {'id': 'ReferenciaDovodPoslednejZmeny',
            'type': 'text',
            'csvindex': 64},
        {'id': 'ReferenciaPolozka',
            'type': 'text',
            'csvindex': 65},
        {'id': 'IdStavVCrz',
            'type': 'integer',
            'csvindex': 66},
    ]

    DATE_ITEM_NAMES = ['DatumVyhlasenia', 'DatumZazmluvnenia', 'LehotaPlneniaOd',
        'LehotaPlneniaDo', 'LehotaPlneniaPresne', 'LehotaNaPredkladaniePonuk',
        'ZaciatokAukcie']
    FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'MaximalnaVyskaZdrojov', 'VstupnaCena',
        'CenaBezDPH', 'CenaSadzbaDPH', 'CenaVrataneDPH', 'Uspora']
    INT_ITEM_NAMES = ['PocetNotifikovanychDodavatelov', 'PocetSutaziacich',
        'PocetPredlozenychPonuk', 'TrvanieAukcie_Minut', 'PredlzovanieAukcie_Minut',
        'IdStavVCrz']


    def update_month(self, csvdate):
        """Overidden to handle transition from old (up to 2018-9) to new structure
        (2018-10 and after). Quite ugly.

        If we no longer need backward compatibility, simply remove this method."""

        update_date = datetime.datetime.strptime(csvdate, '%Y-%m')
        cutoff_date = datetime.datetime(2018, 10, 1)
        result = None
        if cutoff_date <= update_date:
            # new structure => process withy this class
            result = super().update_month(csvdate)
        else:
            # old structure => use old version of this class
            old = ZakazkyAZmluvyOld()
            result = old.update_month(csvdate)

        return result


class ZmluvyOld(EksBaseDatastoreUpdater):
    """Specifics for EKS Zmluvy

    Old version of class valid for up to 2019-9."""

    CONFIG_SECTION = 'zmluvy'
    DIRECTORY_SUBDIR = 'zmluvy'
    CSV_FN_PATTERN = 'ZoznamZmluvReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamZmluvReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'IdentifikatorZmluvy',
            'type': 'text',
            'csvindex': 1},
        {'id': 'ObjednavatelObchodneMeno',
            'type': 'text',
            'csvindex': 2},
        {'id': 'ObjednavatelICO',
            'type': 'text',
            'csvindex': 3},
        {'id': 'ObjednavatelStat',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ObjednavatelObec',
            'type': 'text',
            'csvindex': 5},
        {'id': 'ObjednavatelPSC',
            'type': 'text',
            'csvindex': 6},
        {'id': 'ObjednavatelUlica',
            'type': 'text',
            'csvindex': 7},
        {'id': 'DodavatelObchodneMeno',
            'type': 'text',
            'csvindex': 8},
        {'id': 'DodavatelICO',
            'type': 'text',
            'csvindex': 9},
        {'id': 'DodavatelStat',
            'type': 'text',
            'csvindex': 10},
        {'id': 'DodavatelObec',
            'type': 'text',
            'csvindex': 11},
        {'id': 'DodavatelPSC',
            'type': 'text',
            'csvindex': 12},
        {'id': 'DodavatelUlica',
            'type': 'text',
            'csvindex': 13},
        {'id': 'ZmluvnyVztah',
            'type': 'text',
            'csvindex': 14},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 15},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 16},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 17},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 18},
        {'id': 'MiestoPlneniaStat',
            'type': 'text',
            'csvindex': 19},
        {'id': 'MiestoPlneniaKraj',
            'type': 'text',
            'csvindex': 20},
        {'id': 'MiestoPlneniaOkres',
            'type': 'text',
            'csvindex': 21},
        {'id': 'MiestoPlneniaObec',
            'type': 'text',
            'csvindex': 22},
        {'id': 'MiestoPlneniaUlica',
            'type': 'text',
            'csvindex': 23},
        {'id': 'LehotaPlneniaOd',
            'type': 'timestamp',
            'csvindex': 24},
        {'id': 'LehotaPlneniaDo',
            'type': 'timestamp',
            'csvindex': 25},
        {'id': 'LehotaPlneniaPresne',
            'type': 'timestamp',
            'csvindex': 26},
        {'id': 'MnozstvoJednotka',
            'type': 'text',
            'csvindex': 27},
        {'id': 'MnozstvoHodnota',
            'type': 'float',
            'csvindex': 28},
        {'id': 'CenaBezDPH',
            'type': 'float',
            'csvindex': 29},
        {'id': 'CenaSadzbaDPH',
            'type': 'float',
            'csvindex': 30},
        {'id': 'CenaVrataneDPH',
            'type': 'float',
            'csvindex': 31},
        {'id': 'Uspora',
            'type': 'float',
            'csvindex': 32},
        {'id': 'DatumZazmluvnenia',
            'type': 'timestamp',
            'csvindex': 33},
    ]

    DATE_ITEM_NAMES = ['LehotaPlneniaOd', 'LehotaPlneniaDo', 'LehotaPlneniaPresne',
        'DatumZazmluvnenia']
    FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'CenaBezDPH', 'CenaSadzbaDPH',
        'CenaVrataneDPH', 'Uspora']
    INT_ITEM_NAMES = []


class Zmluvy(EksBaseDatastoreUpdater):
    """Specifics for EKS Zmluvy"""

    CONFIG_SECTION = 'zmluvy'
    DIRECTORY_SUBDIR = 'zmluvy'
    CSV_FN_PATTERN = 'ZoznamZmluvReport_%Y-%m_.csv'
    CSV_FN_PATTERN_2 = 'ZoznamZmluvReport_%s_.csv'

    PRIMARY_KEYS = ['IdentifikatorZakazky']
    STRUCTURE = [
        {'id': 'IdentifikatorZakazky',
            'type': 'text',
            'csvindex': 0},
        {'id': 'IdentifikatorZmluvy',
            'type': 'text',
            'csvindex': 1},
        {'id': 'ObjednavatelObchodneMeno',
            'type': 'text',
            'csvindex': 2},
        {'id': 'ObjednavatelICO',
            'type': 'text',
            'csvindex': 3},
        {'id': 'ObjednavatelStat',
            'type': 'text',
            'csvindex': 4},
        {'id': 'ObjednavatelObec',
            'type': 'text',
            'csvindex': 5},
        {'id': 'ObjednavatelPSC',
            'type': 'text',
            'csvindex': 6},
        {'id': 'ObjednavatelUlica',
            'type': 'text',
            'csvindex': 7},
        {'id': 'DodavatelObchodneMeno',
            'type': 'text',
            'csvindex': 8},
        {'id': 'DodavatelICO',
            'type': 'text',
            'csvindex': 9},
        {'id': 'DodavatelStat',
            'type': 'text',
            'csvindex': 10},
        {'id': 'DodavatelObec',
            'type': 'text',
            'csvindex': 11},
        {'id': 'DodavatelPSC',
            'type': 'text',
            'csvindex': 12},
        {'id': 'DodavatelUlica',
            'type': 'text',
            'csvindex': 13},
        {'id': 'ZmluvnyVztah',
            'type': 'text',
            'csvindex': 14},
        {'id': 'OpisnyFormularNazov',
            'type': 'text',
            'csvindex': 15},
        {'id': 'OpisnyFormularCpv',
            'type': 'text',
            'csvindex': 16},
        {'id': 'OpisnyFormularDruh',
            'type': 'text',
            'csvindex': 17},
        {'id': 'OpisnyFormularKategoriaSluzieb',
            'type': 'text',
            'csvindex': 18},
        {'id': 'MiestoPlneniaStat',
            'type': 'text',
            'csvindex': 19},
        {'id': 'MiestoPlneniaKraj',
            'type': 'text',
            'csvindex': 20},
        {'id': 'MiestoPlneniaOkres',
            'type': 'text',
            'csvindex': 21},
        {'id': 'MiestoPlneniaObec',
            'type': 'text',
            'csvindex': 22},
        {'id': 'MiestoPlneniaUlica',
            'type': 'text',
            'csvindex': 23},
        {'id': 'LehotaPlneniaOd',
            'type': 'timestamp',
            'csvindex': 24},
        {'id': 'LehotaPlneniaDo',
            'type': 'timestamp',
            'csvindex': 25},
        {'id': 'LehotaPlneniaPresne',
            'type': 'timestamp',
            'csvindex': 26},
        {'id': 'MnozstvoJednotka',
            'type': 'text',
            'csvindex': 27},
        {'id': 'MnozstvoHodnota',
            'type': 'float',
            'csvindex': 28},
        {'id': 'CenaBezDPH',
            'type': 'float',
            'csvindex': 29},
        {'id': 'CenaSadzbaDPH',
            'type': 'float',
            'csvindex': 30},
        {'id': 'CenaVrataneDPH',
            'type': 'float',
            'csvindex': 31},
        {'id': 'Uspora',
            'type': 'float',
            'csvindex': 32},
        {'id': 'DatumZazmluvnenia',
            'type': 'timestamp',
            'csvindex': 33},
        {'id': 'IdStavVCrz',
            'type': 'integer',
            'csvindex': 34},
    ]

    DATE_ITEM_NAMES = ['LehotaPlneniaOd', 'LehotaPlneniaDo', 'LehotaPlneniaPresne',
        'DatumZazmluvnenia']
    FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'CenaBezDPH', 'CenaSadzbaDPH',
        'CenaVrataneDPH', 'Uspora']
    INT_ITEM_NAMES = ['IdStavVCrz']


    def update_month(self, csvdate):
        """Overidden to handle transition from old (up to 2018-9) to new structure
        (2018-10 and after). Quite ugly.

        If we no longer need backward compatibility, simply remove this method."""

        update_date = datetime.datetime.strptime(csvdate, '%Y-%m')
        cutoff_date = datetime.datetime(2018, 10, 1)
        result = None
        if cutoff_date <= update_date:
            # new structure => process withy this class
            result = super().update_month(csvdate)
        else:
            # old structure => use old version of this class
            old = ZmluvyOld()
            result = old.update_month(csvdate)

        return result


def help():
    print('use \'setup\' or \'update\' parameter')


if __name__ == '__main__':

    if len(sys.argv) < 2:
        help()
        exit()

    action = sys.argv[1]

    if action not in ('setup', 'update',):
        help()
        exit()

    eks_datasets = [
        AukcnePonuky(),
        KontrakracnePonuky(),
        OpisneFormulare(),
        Referencie(),
        Zakazky(),
        ZakazkyAZmluvy(),
        Zmluvy()
    ]

    if action == 'setup':
        for dataset in eks_datasets:
            dataset.setup()
    elif action == 'update':
        for dataset in eks_datasets:
            dataset.update()
