import logging
import time
from contextlib import closing
from datetime import datetime
import psycopg2
from psycopg2.extras import DictCursor

from dto import FilmworkData, PostgresSettings, ElasticSettings
from elastic_server import ElasticSaver, parse_from_postgres_to_es
from postgres_loader import PostgresLoader
from transfer_state import JsonFileStorage, State
from utils import TransferDataError


if __name__ == '__main__':
    dsl = PostgresSettings().dict()
    es_connect = ElasticSettings().dict()

    while True:
        try:
            with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn, pg_conn.cursor() as curs:
                storage = JsonFileStorage('data.json')
                state = State(storage)
                date_from = state.get_state('date_from')
                offset = state.get_state('offset')

                postgres_loader = PostgresLoader(pg_conn, curs, offset)
                es_saver = ElasticSaver(**es_connect)
                table_names = ['film_work',  'person', 'genre']
                for table_name in table_names:
                    while True:
                        filmwork_ids = postgres_loader.get_filmwork_ids(table_name, date_from)
                        if not filmwork_ids:
                            state.set_state('offset', 0)
                            break
                        filmwork_data = [
                            FilmworkData.parse_obj(data)
                            for data in postgres_loader.get_filmworks_data(filmwork_ids)
                        ]
                        postgres_loader.offset += postgres_loader.LIMIT_ROWS
                        state.set_state('offset', postgres_loader.offset)

                        es_film_works = []
                        for film_id in filmwork_ids:
                            film_data = list(filter(lambda x: x.fw_id == film_id, filmwork_data))
                            es_film_works.append(parse_from_postgres_to_es(film_data, film_id))

                        rows_count, errors = es_saver.send_data_to_es(es_saver.es, es_film_works)

                    state.set_state('offset', 0)

                state.set_state('date_from', datetime.now().strftime('%Y-%m-%d'))

        except psycopg2.OperationalError:
            logging.error('Error connecting to Postgres database')

        except TransferDataError:
            logging.error('Finish transfer due to TransferDataError')

        time.sleep(1)
