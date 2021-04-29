import csv
import shutil
import time
from os import path


class Exports(object):
    def __init__(self, client):
        self.client = client

    def create(self, **kwargs):
        response = self._post(path='/do/create', params=kwargs)
        export_create_result = response.get('export')
        return export_create_result

    def status(self, id):
        response = self._get(path='/do/read/id/{id}'.format(id=id))
        return response

    def results(self, id, file_id, local_filepath):
        all_records = []
        response = self._get(path=f'/do/downloadResults/id/{id}/file/{file_id}')
        tmp_file_name = f"raw_export_{int(time.time())}.csv"
        tmp_file_name = path.join(local_filepath, tmp_file_name)
        with open(tmp_file_name, 'w') as result_file:
            response.seek(0)
            shutil.copyfileobj(response, result_file)

        with open(tmp_file_name, 'r') as result_file:
            csv_reader = csv.DictReader(result_file)
            for data_row in csv_reader:
                all_records.append(data_row)
        return all_records


    def _get(self, object_name='export', path=None, params=None):
        """GET requests for the Visitor Activity object."""
        if params is None:
            params = {}
        response = self.client.get(object_name=object_name, path=path, params=params)
        return response

    def _post(self, object_name='export', path=None, params=None):
        """POST requests for the Visitor Activity object."""
        if params is None:
            params = {}
        response = self.client.post(object_name=object_name, path=path, params=params)
        return response