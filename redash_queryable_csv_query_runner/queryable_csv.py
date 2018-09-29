import csv
import json
import sqlite3
from contextlib import closing

from dateutil import parser

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

    def run_query(self, query, user):
        query = query.strip()
        path = str(self.configuration.get('path'))
        delimiter = str(self.configuration.get('delimiter', ','))
        if delimiter == 'TAB':
            delimiter = "\t"

        columns = []
        rows = []

        with open(path) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            columns = reader.fieldnames
            for row in reader:
                rows.append(row)

        create_table = u'CREATE TABLE csv ({columns});'.format(
            columns=','.join(columns))
        insert_template = u'INSERT INTO csv ({column_list}) VALUES ({place_holders})'.format(
            column_list=','.join(columns),
            place_holders=','.join([':{}'.format(column) for column in columns]))

        with closing(sqlite3.connect(':memory:')) as conn:
            conn.execute(create_table)

            for row in rows:
                conn.execute(insert_template, row)

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

            return json.dumps({'columns': columns, 'rows': rows}, cls=JSONEncoder), None


register(QueryableCsv)
