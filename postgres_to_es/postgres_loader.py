from typing import Optional

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

    def get_ids_from_table(self, table_name: str, modified: str):
        query = f'SELECT id from {table_name} ' \
                f"WHERE modified > '{modified}' " \
                'ORDER BY modified '
        if table_name != 'genre':
            query += f'LIMIT {self.LIMIT_ROWS} OFFSET {self.offset}'
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids_by_persons(self, persons_ids: tuple[str]) -> tuple[str]:
        query = 'SELECT fw.id FROM film_work fw ' \
                'LEFT JOIN person_film_work pfw ON pfw.film_work_id = fw.id ' \
                f'WHERE pfw.person_id IN {persons_ids} ' \
                'ORDER BY fw.modified'
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids_by_genre(self, genres_ids: tuple[str]) -> tuple[str]:
        query = 'SELECT fw.id FROM film_work fw ' \
                'LEFT JOIN genre_film_work gfw ON gfw.film_work_id = fw.id ' \
                f'WHERE gfw.genre_id in {genres_ids} ' \
                'ORDER BY fw.modified ' \
                f'LIMIT {self.LIMIT_ROWS} OFFSET {self.offset}'
        data = self.extract_data(query, self.curs)
        return tuple([item[0] for item in data]) if data else tuple()

    def get_filmwork_ids(self, table_name: str, modified: str) -> Optional[tuple[str]]:
        objects_ids = self.get_ids_from_table(table_name, modified)
        if table_name == 'film_work':
            return objects_ids
        elif table_name == 'person':
            return self.get_filmwork_ids_by_persons(objects_ids) if objects_ids else None
        elif table_name == 'genre':
            return self.get_filmwork_ids_by_genre(objects_ids) if objects_ids else None

    def get_filmworks_data(self, filmwork_ids: tuple[str]) -> list:
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
