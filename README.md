eks-od-datastore-pusher
=======================

This scripts takes individual CSV files harvested from portal.eks.sk (EKS)
(with [eks-od-harvestrer](https://github.com/OpenDataSk/eks-od-harvestrer))
and uploads them into single resource in CKAN. Intended side-effects of that
are:

- merging of the individual files into one big table
- calculation of "last modified" timestamps, since EKS is not providing
  those

# Basic information

TODO

## Usage

* Clone this repository:

        git clone https://github.com/OpenDataSk/eks-od-datastore-pusher.git
        cd eks-od-datastore-pusher

* Create a virtualenv and install dependencies:

        virtualenv .env
        source .env/bin/activate
        pip install -r requirements.txt

* Define your CKAN URL and API key in the `config.ini` file:

	cp config.ini.template config.ini
	...

* Run the setup command, and write the resulting resource id in your `config.ini` file:

        python datastore_update.py setup

* Run the update command:

        python datastore_update.py update

You probably want to set up this command to run hourly, eg with a cron job:

    crontab -e

Add a line like this:

    0 0 * * * /path/to/your/pyenv/bin/python /path/to/your/workspace/eks-od-datastore-pusher/datastore_updater.py update

# TODO

- finish initial basic feature(s)
- add license (for now BSD preffered)
- review this README and adjust all the stuff copied from ckan/example-earthquake-datastore which does not fit the fork
- ...

# Detailed technical stuff

**Note**: Requires [requests](http://docs.python-requests.org/).

An example script that sets up and periodically updates a
[CKAN DataStore](http://docs.ckan.org/en/latest/datastore.html) table
with [earthquake data](http://earthquake.usgs.gov) from the NGDS.

This example demonstrates how to use DataStore tables to push data directly
to them rather than automatically import tabular files via the DataPusher.
It can be easily be adapted to different data sources.

See it in action at http://demo.ckan.org/dataset/ngds-earthquakes-data

## How it works

When running the `setup` command we are doing the following things:

* Creating a new dataset in the remote CKAN instance using the [package_create](http://docs.ckan.org/en/latest/api/index.html#ckan.logic.action.create.package_create) API action.

* Getting a dump of the remote earthquake data for the past day and extracting the records we want to push to the DataStore.

* Preparing a mapping of the table fields with the correct field types to ensure they are handled correctly by the DataStore.

* Pushing the prepared records and the field mapping to a new DataStore resource on the previously created dataset, using the [datastore_create](http://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_create) API action. Note how we use the id of the previously created dataset. The new resource will be of type `datastore` and will offer a CSV dump of the data stored in the DataStore.

Once we have this initial setup we can use the `update` command to periodically request updated earthquake data and push it to our DataStore table using the [datastore_upsert](http://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_upsert) API action.
As we defined a primary key when creating the DataStore table we can use the `upsert` method, which will update existing records and insert any new ones.

When accessed via the CKAN frontend, the data can be explored in the grid and map previews powered by Recline, and of course it can be accessed programmatically from other applications using the [datastore_search](http://docs.ckan.org/en/latest/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search) API action.
