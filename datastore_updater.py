import sys
import configparser
import json
import datetime


import requests

usage = '''

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


def exit(msg=usage):
    print(msg)
    sys.exit(1)


def _get_records(earthquake_data):
    records = []
    if len(earthquake_data['features']):
        for feature in earthquake_data['features']:
            record = feature['properties']
            record.update({
                'longitude': feature['geometry']['coordinates'][0],
                'latitude': feature['geometry']['coordinates'][1],
                'added': datetime.datetime.now().isoformat(),
            })
            records.append(record)
    return records


def setup(config):

    ckan_url = config.get('main', 'ckan_url').rstrip('/')
    api_key = config.get('main', 'api_key')

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

    response = requests.post('{0}/api/action/package_create'.format(ckan_url),
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

    response = requests.post('{0}/api/action/datastore_create'.format(ckan_url),
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


def update(config):

    ckan_url = config.get('main', 'ckan_url').rstrip('/')
    api_key = config.get('main', 'api_key')

    resource_id = config.get('main', 'resource_id')
    if not resource_id:
        exit('You need to add the resource id to your configuration file.\n' +
             'Did you run `datastore_update.py setup`first?')

    # Get the latest Earthquake data
    past_hour_earthquake_data = requests.get(PAST_HOUR_DATA_URL).json()
    records = _get_records(past_hour_earthquake_data)

    if len(records) == 0:
        # No new records
        return

    # Push the records to the DataStore table
    data = {
        'resource_id': resource_id,
        'method': 'upsert',
        'records': records,
    }

    response = requests.post('{0}/api/action/datastore_upsert'.format(ckan_url),
                             data=json.dumps(data),
                             headers={'Content-type': 'application/json',
                                      'Authorization': api_key},)

    if response.status_code != 200:
        exit('Error: {0}'.format(response.content))

    return


if __name__ == '__main__':

    if len(sys.argv) < 2:
        exit()

    action = sys.argv[1]

    if action not in ('setup', 'update',):
        exit()

    config = configparser.SafeConfigParser()
    config.read('config.ini')
    for key in ('ckan_url', 'api_key',):
        if not config.get('main', key):
            exit('Please fill the {0} option in the config.ini file'
                 .format(key))

    if action == 'setup':
        setup(config)
    elif action == 'update':
        update(config)
