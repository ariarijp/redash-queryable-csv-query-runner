import csv
import json
import sqlite3
from contextlib import closing

from redash.query_runner import *
from redash.utils import JSONEncoder


def _guess_type(value):
    return TYPE_STRING


class QueryableCsv(BaseSQLQueryRunner):
    @classmethod
    def name(cls):
        return 'Queryable CSV (Unofficial)'

    @classmethod
    def configuration_schema(cls):
        return {
            'type': 'object',
            'properties': {
                'path': {
                    'type': 'string',
                    'title': 'Path',
                },
                'delimiter': {
                    'type': 'string',
                    'title': 'Delimiter',
                    'default': ',',
                },
            },
        }

    @classmethod
    def type(cls):
        return 'queryable_csv'

    @classmethod
    def annotate_query(cls):
        return False

    def _get_tables(self, schema):
        if 'csv' not in schema:
            schema['csv'] = {'name': 'csv', 'columns': []}

        return schema.values()

    def test_connection(self):
        pass

    def _read_csv(self, path, delimiter):
        rows = []

        with open(path) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            columns = reader.fieldnames
            for row in reader:
                rows.append(row)

        return columns, rows

    def _load_csv_to_table(self, conn, path, delimiter):
        columns, rows = self._read_csv(path, delimiter)

        create_table = u'CREATE TABLE csv ({columns});'.format(
            columns=','.join(columns))
        insert_template = u'INSERT INTO csv ({column_list}) VALUES ({place_holders})'.format(
            column_list=','.join(columns),
            place_holders=','.join([':{}'.format(column) for column in columns]))

        conn.execute(create_table)

        for row in rows:
            conn.execute(insert_template, row)

    def _execute_query(self, conn, query):
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            columns = self.fetch_columns(
                [(d[0], None) for d in cursor.description])

            rows = []
            column_names = [c['name'] for c in columns]

            for row in cursor:
                for i, col in enumerate(row):
                    guess = _guess_type(col)

                    if columns[i]['type'] is None:
                        columns[i]['type'] = guess
                    elif columns[i]['type'] != guess:
                        columns[i]['type'] = TYPE_STRING

                rows.append(dict(zip(column_names, row)))

        return {'columns': columns, 'rows': rows}

    def run_query(self, query, user):
        query = query.strip()
        path = str(self.configuration.get('path'))
        delimiter = str(self.configuration.get('delimiter', ','))
        if delimiter == 'TAB':
            delimiter = "\t"

        with closing(sqlite3.connect(':memory:')) as conn:
            self._load_csv_to_table(conn, path, delimiter)

            results = self._execute_query(conn, query)

        return json.dumps(results, cls=JSONEncoder), None


register(QueryableCsv)
