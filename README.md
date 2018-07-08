# Redash Queryable CSV Query Runner

## Installation

```
$ pip install git+https://github.com/ariarijp/redash-queryable-csv-query-runner
```

Then, add `redash_csv_parser_query_runner` to `REDASH_ADDITIONAL_QUERY_RUNNERS` in `.env` file or something like that.

```
export REDASH_ADDITIONAL_QUERY_RUNNERS="redash_queryable_csv_query_runner.queryable_csv"
```

Finally, restart Redash processes.

```
$ sudo service supervisor restart
```

## Usage

### Data Source options

* Path (Required)
  * e.g. `/path/to/data.csv`
* Delimiter (Required)
  * e.g. `,`

## License

MIT

## Author

[ariarijp / Takuya Arita](https://github.com/ariarijp)
