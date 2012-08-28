from __future__ import unicode_literals

from norm import RowsProxy


def make_rows():
    return [
        {'user_id': 5, 'opponent_name': 'John', 'game_id': 55, 'score': 34.14},
        {'user_id': 5, 'opponent_name': 'John', 'game_id': 57, 'score': 35.14},
        {'user_id': 5, 'opponent_name': 'Dirk', 'game_id': 59, 'score': 37.14},
        {'user_id': 5, 'opponent_name': 'Dirk', 'game_id': 60, 'score': 38.14},
        {'user_id': 6, 'opponent_name': 'Gabe', 'game_id': 95, 'score': 32.14},
        {'user_id': 6, 'opponent_name': 'Gabe', 'game_id': 31, 'score': 31.14},
        {'user_id': 6, 'opponent_name': 'Ted', 'game_id': 5, 'score': 4.14},
        {'user_id': 7, 'opponent_name': 'Jim', 'game_id': 27, 'score': 8.14}]


def test_grouping():
    rp = RowsProxy(make_rows())
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
