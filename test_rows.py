from __future__ import unicode_literals

from norm import RowsProxy

column_names = ('user_id', 'opponent_name', 'game_id', 'score')


def make_rows():
    return [
        (5, 'John', 55, 34.14),
        (5, 'John', 57, 35.14),
        (5, 'Dirk', 59, 37.14),
        (5, 'Dirk', 60, 38.14),
        (6, 'Gabe', 95, 32.14),
        (6, 'Gabe', 31, 31.14),
        (6, 'Ted', 5, 4.14),
        (7, 'Jim', 27, 8.14)]


def test_grouping():
    rp = RowsProxy(make_rows(), column_names)
    id_score_map = {}
    for user_id, rows in rp('user_id'):
        scores = []
        for row in rows:
            scores.append(row.get('score'))
        id_score_map[user_id] = scores

    assert id_score_map == {
        5: [34.14, 35.14, 37.14, 38.14],
        6: [32.14, 31.14, 4.14],
        7: [8.14]}

    unique_games = []
    for (user_id, opponent_name), rows in rp('user_id', 'opponent_name'):
        unique_games.append((user_id, opponent_name))
    assert unique_games == [
        (5, 'John'),
        (5, 'Dirk'),
        (6, 'Gabe'),
        (6, 'Ted'),
        (7, 'Jim')]
