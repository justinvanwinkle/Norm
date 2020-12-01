

class QueryRequirement:
    def __init__(self, requires):
        self.requires = requires


class row_method:
    def __init__(self, requirements):
        self.requirements = requirements

    def __call__(self, wrapped):
        pass

# blah blah blah


@row_method(name='users.name')
def is_bill(name):
    if name.lower() in ('bill', 'william', 'billy'):
        return True
    return False


@row_method('users.user_id = scores.user_id',
            'scores.game_id = games.game_id',
            game_name='games.game_name',
            username='users.username')
def rankings_url(game_name, username):
    return f'http://gameapp.com/user_scores/{username}/{game_name}'


user_high_scores_q = """
SELECT u.name AS the_user_name,
       e.active_email
  FROM users u
  JOIN emails e
       ON u.user_id, = e.user_id
 WEHRE e.active = true;"""

user_losses_q = """
SELECT u.user_id,
       s.game_id
  FROM users u
  JOIN scores s
       ON u.user_id = s.user_id
 WHERE game_results.result = 'LOSS';"""

row = run_query(user_high_scores_q)[0]
print(row.is_bill())  # fine, prints True or False
print(row.rankings_url())  # AttributeError, doesn't satisfy any methods

row = run_query(user_losses_q)[0]
row.rankings_url()  # AttributeError

@row_method('users.user_id = scores.user_id',
            user_id='users.user_id',
            game_id='scores.game_id')
def rankings_url(user_id, game_id):
    return f'http://gameapp.com/user_scores?{user_id=}&{game_id=}'

row.rankings_url()  # fine now
