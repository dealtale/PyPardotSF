import csv
import shutil
import time
from os import path
from io import StringIO


class Exports(object):
    def __init__(self, client):
        self.client = client

    def create(self, request_body):
        response = self._post(path='/do/create', json=request_body)
        export_create_result = response.get('export')
        return export_create_result

    def status(self, id):
        response = self._get(path='/do/read/id/{id}'.format(id=id))
        export_status_result = response.get('export')
        return export_status_result

    def results(self, id, file_id, local_filepath):
        all_records = []
        response = self._get(path=f'/do/downloadResults/id/{id}/file/{file_id}')
        response = response.content
        export_file_contents = response.decode("utf-8")
        export_file_contents = StringIO(export_file_contents)
        tmp_file_name = f"raw_export_{int(time.time())}.csv"
        tmp_file_name = path.join(local_filepath, tmp_file_name)
        with open(tmp_file_name, 'w') as result_file:
            export_file_contents.seek(0)
            shutil.copyfileobj(export_file_contents, result_file)

        with open(tmp_file_name, 'r') as result_file:
            csv_reader = csv.DictReader(result_file)
            for data_row in csv_reader:
                all_records.append(data_row)
        return all_records


    def _get(self, object_name='export', path=None, params=None):
        if params is None:
            params = {}
        response = self.client.get(object_name=object_name, path=path, params=params)
        return response

    def _post(self, object_name='export', path=None, json=None):
        response = self.client.post(object_name=object_name, path=path, json=json)
        return response