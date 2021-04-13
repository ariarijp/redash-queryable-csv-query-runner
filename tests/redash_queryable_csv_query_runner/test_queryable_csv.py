from redash.query_runner import TYPE_FLOAT, TYPE_INTEGER, TYPE_STRING

from redash_queryable_csv_query_runner.queryable_csv import _guess_column_types, _guess_type, _normalize_fieldnames


def test_guess_type():
    assert TYPE_INTEGER == _guess_type('42')
    assert TYPE_FLOAT == _guess_type('3.1415')
    assert TYPE_STRING == _guess_type('spam')


def test_guess_column_types():
    expected = [
        {'name': 'foo', 'type': TYPE_INTEGER},
        {'name': 'bar', 'type': TYPE_FLOAT},
        {'name': 'baz', 'type': TYPE_STRING},
    ]
    actual = _guess_column_types(row=['42', '3.1415', 'spam'],
                                 columns=[
                                     {'name': 'foo'},
                                     {'name': 'bar'},
                                     {'name': 'baz'},
                                 ])

    assert expected == actual


def test_normalize_fieldnames():
    assert ['The_quick_brown_fox_jumps_over_the_lazy_dog'] \
           == _normalize_fieldnames(['The quick brown fox jumps over the lazy dog'])
    assert ['The_quick_br_wn_f_x_jumps__ver_the___zy_d_g'] \
           == _normalize_fieldnames(['The-quick-br.wn-f.x-jumps-.ver-the-|@zy-d.g'])
