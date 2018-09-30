import csv
import json
import re
import sqlite3
from contextlib import closing

from redash.query_runner import *
from redash.utils import JSONEncoder


def _guess_type(value):
    try:
        int(value)
        return TYPE_INTEGER
    except:
        pass

    try:
        float(value)
        return TYPE_FLOAT
    except:
        pass

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

    def _normalize_fieldnames(self, fieldnames):
        return [re.sub('[^0-9a-zA-Z_]+', '_', fieldname) for fieldname in fieldnames]

    def _read_csv(self, path, delimiter):
        rows = []

        with open(path) as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            reader.fieldnames = self._normalize_fieldnames(reader.fieldnames)
            columns = reader.fieldnames
            for row in reader:
                rows.append(row)

        return columns, rows

    def _create_table(self, conn, columns, row):
        column_defs = []
        for column in columns:
            value = row[column]
            type = _guess_type(value)
            column_def = '`{}` TEXt'.format(column)
            if type == TYPE_FLOAT:
                column_def = '`{}` REAL'.format(column)
            elif type == TYPE_INTEGER:
                column_def = '`{}` INTEGER'.format(column)

            column_defs.append(column_def)

        create_table = u'CREATE TABLE csv ({columns});'.format(
            columns=','.join(column_defs))

        conn.execute(create_table)

    def _load_csv_to_table(self, conn, path, delimiter):
        columns, rows = self._read_csv(path, delimiter)

        self._create_table(conn, columns, rows[0])

        insert_template = u'INSERT INTO csv ({column_list}) VALUES ({place_holders})'.format(
            column_list=','.join(columns),
            place_holders=','.join([':{}'.format(column) for column in columns]))

        for row in rows:
            conn.execute(insert_template, row)

    def _guess_column_types(self, row, columns):
        for i, value in enumerate(row):
            columns[i]['type'] = _guess_type(value)

        return columns

    def _execute_query(self, conn, query):
        with closing(conn.cursor()) as cursor:
            cursor.execute(query)

            columns = self.fetch_columns(
                [(d[0], None) for d in cursor.description])

            rows = []
            column_names = [c['name'] for c in columns]

            for i, row in enumerate(cursor):
                if i == 0:
                    columns = self._guess_column_types(row, columns)

                rows.append(dict(zip(column_names, row)))

        return {'columns': columns, 'rows': rows}

    def run_query(self, query, user):
        query = query.strip()
        path = str(self.configuration.get('path'))
        delimiter = str(self.configuration.get('delimiter', ','))
        if delimiter == 'TAB':
            delimiter = "\t"

        with closing(sqlite3.connect(':memory:')) as conn:
            conn.text_factory = str
            self._load_csv_to_table(conn, path, delimiter)

            results = self._execute_query(conn, query)

        return json.dumps(results, cls=JSONEncoder), None


register(QueryableCsv)
