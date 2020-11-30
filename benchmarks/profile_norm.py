from norm.norm_psycopg2 import PG_SELECT as SELECT


def norm_bench():
    s = (SELECT('users.name',
                'users.fullname',
                'addresses.email_address')
         .FROM('users')
         .JOIN('addresses', ON='users.id = addresses.user_id')
         .WHERE('users.id > %(user_id)s').bind(user_id=1)
         .WHERE("users.name LIKE %(name)s")
         .bind(name='Justin%'))

    return s.query, s.binds


def main():
    for _ in range(100000):
        norm_bench()


if __name__ == '__main__':
    main()
