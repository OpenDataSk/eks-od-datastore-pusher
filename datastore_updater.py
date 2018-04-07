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

#PAST_DAY_DATA_URL = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson'
#PAST_HOUR_DATA_URL = 'http://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
BATCH_SIZE = 100
STATE_FILE = 'datastore_updater.state'

# state keys
STATE_LAST_PROCESSED = 'last_processed'


# structure description
ZAZKAZKY_STRUCTURE = [
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


class EksZakazkyDatastoreUpdater:

    def __init__(self):
        self.state = {}
        self.load_state()

        config = configparser.SafeConfigParser()
        config.read('config.ini')
        for key in ('ckan_url', 'api_key',):
            if not config.get('main', key):
                exit('Please fill the {0} option in the config.ini file'
                     .format(key))

        self.ckan_url = config.get('main', 'ckan_url').rstrip('/')
        self.api_key = config.get('main', 'api_key')

        self.resource_id = config.get('main', 'resource_id')
        if not self.resource_id:
            exit('You need to add the resource id to your configuration file.\n' +
                 'Did you run `datastore_update.py setup`first?')

        self.directory_zakazky = config.get('main', 'directory_zakazky')
        if not self.resource_id:
            exit('You need to add the path to directory with "Zakazky" files ' +
                 'to your configuration file.')


    def load_state(self):
        if not os.path.isfile(STATE_FILE):
            print('info: no previous state found (%s)' % STATE_FILE)
            return

        state_file = open(STATE_FILE, "rb");
        self.state = pickle.load(state_file);


    def save_state(self):
        state_file = open(STATE_FILE, "wb");
        pickle.dump(self.state, state_file);


    def exit(self, msg=USAGE):
        print(msg)
        sys.exit(1)


    def setup(self):
        """Basic setup operation called from command line."""

        # Create a dataset first
        data = {
            'name': 'eks-zakazky-datapusher-test5',
            'title': 'EKS - Zakázky - datapusher test',
            'owner_org': 'opendata_sk',	# TODO: take that from config.ini
            'notes': '''
Target for https://github.com/OpenDataSk/eks-od-datastore-pusher during development and testing. Thus:

- it may contain bogus data
- data may vanish without warning
- BEWARE OF DRAGONS
            ''',
        }

        response = requests.post(
            '{0}/api/action/package_create'.format(ckan_url),
            data=json.dumps(data),
            headers={'Content-type': 'application/json',
                     'Authorization': api_key},
            # FIXME: security vulnerability => move this to confing.ini so that those using self-signed certs can get stuff woring but those with good certs can by default be safe!!!
            # (reference: http://docs.python-requests.org/en/master/user/advanced/?highlight=ssl#ssl-cert-verification)
            verify=False)

        if response.status_code != 200:
            exit('Error creating dataset: {0}'.format(response.content))

        dataset_id = response.json()['result']['id']

        # Then create a resource, for now empty
        records = []

        # Manually set the field types to ensure they are handled properly
        # TODO: Those fileds are for "Zakazky". Later we will enhance that also for other EKS sets (Zmluvy, ...)
        # TODO: derive that from ZAZKAZKY_STRUCTURE!!!
        fields = [
            {'id': 'IdentifikatorZakazky', 'type': 'text'},
            {'id': 'ZakazkaUrl', 'type': 'text'},
            {'id': 'StavZakazky', 'type': 'text'},
            {'id': 'PouzityPostup', 'type': 'text'},
            {'id': 'ObjednavatelDruh', 'type': 'text'},
            {'id': 'ObjednavatelObchodneMeno', 'type': 'text'},
            {'id': 'ObjednavatelICO', 'type': 'text'},
            {'id': 'ObjednavatelStat', 'type': 'text'},
            {'id': 'ObjednavatelObec', 'type': 'text'},
            {'id': 'ObjednavatelPSC', 'type': 'text'},
            {'id': 'ObjednavatelUlica', 'type': 'text'},
            {'id': 'DatumVyhlasenia', 'type': 'timestamp'},
            {'id': 'DatumZazmluvnenia', 'type': 'timestamp'},
            {'id': 'OpisnyFormularNazov', 'type': 'text'},
            {'id': 'OpisnyFormularKlucoveSlova', 'type': 'text'},
            {'id': 'OpisnyFormularCpv', 'type': 'text'},
            {'id': 'OpisnyFormularDruh', 'type': 'text'},
            {'id': 'OpisnyFormularKategoriaSluzieb', 'type': 'text'},
            {'id': 'OpisnyFormularFunkcnaSpecifikacia', 'type': 'text'},
            {'id': 'OpisnyFormularTechnickaSpecifikaciaTextova', 'type': 'text'},
            {'id': 'OpisnyFormularTechnickaSpecifikaciaCiselna', 'type': 'text'},
            {'id': 'MiestoPlneniaStat', 'type': 'text'},
            {'id': 'MiestoPlneniaKraj', 'type': 'text'},
            {'id': 'MiestoPlneniaOkres', 'type': 'text'},
            {'id': 'MiestoPlneniaObec', 'type': 'text'},
            {'id': 'MiestoPlneniaUlica', 'type': 'text'},
            {'id': 'LehotaPlneniaOd', 'type': 'timestamp'},
            {'id': 'LehotaPlneniaDo', 'type': 'timestamp'},
            {'id': 'LehotaPlneniaPresne', 'type': 'timestamp'},
            {'id': 'MnozstvoJednotka', 'type': 'text'},
            {'id': 'MnozstvoHodnota', 'type': 'float'},
            {'id': 'MaximalnaVyskaZdrojov', 'type': 'float'},
            {'id': 'ZmluvnyVztah', 'type': 'text'},
            {'id': 'FinancovanieEU', 'type': 'bool'},
            {'id': 'HodnotiaceKriterium', 'type': 'text'},
            {'id': 'LehotaNaPredkladaniePonuk', 'type': 'timestamp'},
            {'id': 'PocetNotifikovanychDodavatelov', 'type': 'integer'},
            {'id': 'VstupnaCena', 'type': 'float'},
            {'id': 'PocetSutaziacich', 'type': 'integer'},
            {'id': 'PocetPredlozenychPonuk', 'type': 'integer'},
            {'id': 'ZaciatokAukcie', 'type': 'timestamp'},
            {'id': 'TrvanieAukcie_Minut', 'type': 'integer'},
            {'id': 'PredlzovanieAukcie_Minut', 'type': 'integer'},
            {'id': 'ProtokolOPriebehuZadavaniaZakazky', 'type': 'text'},
            {'id': 'Priloha_c1_ZmluvnyFormularZakazky', 'type': 'text'},
            {'id': 'Priloha_c2_VyslednePoradieDodavatelov', 'type': 'text'},
            {'id': 'Priloha_c3_Zmluva', 'type': 'text'},
            {'id': 'Priloha_c4A_ZaznamOSystemovychUdalostiachZakazky', 'type': 'text'},
            {'id': 'Priloha_c4B_ZaznamOSystemovychUdalostiachElektronickejAukcie', 'type': 'text'},
            {'id': 'AnonymnyZmluvnyFormularZakazky', 'type': 'text'},
            {'id': 'ObjednavkovyFormularZakazky', 'type': 'text'},
        ]

        # Push the records to the DataStore table. This will create a resource
        # of type datastore.
        data = {
            'resource': {
                'package_id': dataset_id,
                'name': 'Zakazky',
                'format': 'csv',
                'notes': '''
Set of multiple CSVs merged together into one complete resource.

TODO: further details
                '''
            },
            'records': records,
            'fields': fields,
            'primary_key': ['IdentifikatorZakazky'],
        }

        response = requests.post(
            '{0}/api/action/datastore_create'.format(ckan_url),
            data=json.dumps(data),
            headers={'Content-type': 'application/json',
                     'Authorization': api_key},
            # FIXME: security vulnerability => move this to confing.ini so that those using self-signed certs can get stuff woring but those with good certs can by default be safe!!!
            # (reference: http://docs.python-requests.org/en/master/user/advanced/?highlight=ssl#ssl-cert-verification)
            verify=False)

        if response.status_code != 200:
            exit('Error: {0}'.format(response.content))

        resource_id = response.json()['result']['resource_id']
        print('''
Dataset and DataStore resource successfully created with {0} records.
Please add the resource id to your ini file:

resource_id={1}
        '''.format(len(records), resource_id))


    def find_oldest_csvdate(self):
        """Find oldest CSV file in the given directory.

        "Oldest" does not mean selection based on file modification time but instead
        based on year and month embedded in the file names.

        For "Zakazky", following file naming is used:

            ZoznamZakaziekReport_2018-3_.csv
            ZoznamZakaziekReport_2018-4_.csv

        So here, '2018-3' (a.k.a. "CVS date") would be returned."""

        # list the self.directory_zakazky, skip directories, parse out
        # available dates (YYYY-M) from file names
        file_dates = []
        print(os.listdir(self.directory_zakazky))
        for diritem in os.listdir(self.directory_zakazky):
            if not os.path.isfile(os.path.join(self.directory_zakazky, diritem)):
                continue
            try:
                zdate = datetime.datetime.strptime(diritem, 'ZoznamZakaziekReport_%Y-%m_.csv')
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


    @staticmethod
    def csv_header_check(row):
        """Validate a header of CSV file, i.e. fail-safe check which should prevent
        the script from loading improper data into datastore.

        Checks should be sufficient to catch at least:
        1) wrong CSV file (i.e. something completely unrelated)
        2) CSV with new/changed structure (EKS may change stuff)"""

        # Length is -1 because EKS puts separator ',' at the end which creates
        # one more empty column.
        hlen = len(row) - 1
        if hlen != len(ZAZKAZKY_STRUCTURE):
            print('error: %d items in header found, %d expected' % (hlen, len(ZAZKAZKY_STRUCTURE)))
            return False

        for sitem in ZAZKAZKY_STRUCTURE:
            # We strip \ufeff because it's in the CSV file header, thus
            # "damaging" name of first column.
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
            # FIXME: security vulnerability => move this to confing.ini so that those using self-signed certs can get stuff woring but those with good certs can by default be safe!!!
            # (reference: http://docs.python-requests.org/en/master/user/advanced/?highlight=ssl#ssl-cert-verification)
            verify=False)

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
        for item in ZAZKAZKY_STRUCTURE:
            mapping[item['id']] = item['csvindex']
        DATE_ITEM_NAMES = ['DatumVyhlasenia', 'DatumZazmluvnenia', 'LehotaPlneniaOd',
            'LehotaPlneniaDo', 'LehotaPlneniaPresne', 'LehotaNaPredkladaniePonuk',
            'ZaciatokAukcie']
        FLOAT_ITEM_NAMES = ['MnozstvoHodnota', 'MaximalnaVyskaZdrojov', 'VstupnaCena']

        # records to be inserted
        records = []

        # Load the CSV file
        csvfn = '%s/ZoznamZakaziekReport_%s_.csv' % (self.directory_zakazky, csvdate)
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
                for mitem in DATE_ITEM_NAMES:
                    rowjson[mitem] = self.convert_date(row[mapping[mitem]])
                for mitem in FLOAT_ITEM_NAMES:
                    rowjson[mitem] = self.convert_float(row[mapping[mitem]])

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


        # upsert the remainer of records, mark state
        self.upsert(records)
        self.state[STATE_LAST_PROCESSED] = csvdate
        self.save_state()

        print('DataStore resource successfully updated with %d records.' % counter)

        return True


    def update(self):
        """Basic update operation called from command line."""

        # Load "state" (YYYY-M of last processed file); if not then
        month_to_process = None
        if STATE_LAST_PROCESSED in self.state:
            month_to_process = self.state[STATE_LAST_PROCESSED]
        if month_to_process is None:
            month_to_process = self.find_oldest_csvdate()

        # process "last processed" month assuming:
        # 1) if it also still "current month": we will process all, pick
        # updates, re-process again items/line maybe needlessly (but such
        # waste is considered OK while it helps avoid more code)
        # 2) if it is "last month": we weill process it "for the last time",
        # picking up latest updates and then proceed to the next (i.e.
        # current) month
        counter = 0
        while self.update_month(month_to_process):
            counter += 1
            # OK, get the name for "next month" and try it ...
            month_to_process = self.next_csvdate(month_to_process)

        print('%d files processed.' % counter)

        return


if __name__ == '__main__':

    if len(sys.argv) < 2:
        exit()

    action = sys.argv[1]

    if action not in ('setup', 'update',):
        exit()

    eks_zakazky = EksZakazkyDatastoreUpdater()

    if action == 'setup':
        eks_zakazky.setup()
    elif action == 'update':
        eks_zakazky.update()
