from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from utils import backoff


class PostgresLoader:
    LIMIT_ROWS = 100

    def __init__(self, pg_conn: _connection, curs: DictCursor, offset: int):
        self.pg_conn = pg_conn
        self.curs = curs
        self.curs.execute("SET search_path TO content;")
        self.offset = offset

    @staticmethod
    @backoff()
    def extract_data(query: str, curs: DictCursor) -> list:
        curs.execute(query)
        data = curs.fetchall()
        return data

    def get_filmwork_ids(self, modified: str) -> tuple[str]:
        query = 'SELECT id from film_work ' \
                f"WHERE modified > '{modified}' " \
                'ORDER BY modified ' \
                f'LIMIT {self.LIMIT_ROWS} OFFSET {self.offset}'
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmworks_data(self, filmwork_ids: tuple[str]):
        query = 'SELECT fw.id as fw_id, fw.title, fw.description, ' \
                'fw.rating, fw.type, fw.created, fw.modified, pfw.role, ' \
                'p.id as person_id, p.full_name, g.name as genre_name FROM film_work fw ' \
                'LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id ' \
                'LEFT JOIN person p ON p.id = pfw.person_id ' \
                'LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id ' \
                'LEFT JOIN genre g ON g.id = gfw.genre_id ' \
                f'WHERE fw.id IN {filmwork_ids};'
        data = self.extract_data(query, self.curs)
        return data
