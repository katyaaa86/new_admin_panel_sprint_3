from elasticsearch import Elasticsearch, helpers

from dto import ESFilmworkData, FilmworkData
from utils import backoff


class ElasticSaver:
    def __init__(self, es_host: str, es_user: str, es_password: str):
        self.es = Elasticsearch(es_host, basic_auth=(es_user, es_password), verify_certs=False)

    @staticmethod
    @backoff()
    def send_data_to_es(es: Elasticsearch, es_data: list[ESFilmworkData]) -> tuple[int, list]:
        query = [{'_index': 'movies', '_id': data.id, '_source': data.dict()} for data in es_data]
        rows_count, errors = helpers.bulk(es, query)
        if errors:
            print(errors)
        return rows_count, errors


def parse_from_postgres_to_es(film_data: list[FilmworkData], film_id: str) -> ESFilmworkData:
    actors = {}
    writers = {}
    directors = {}
    genres = set()
    for row in film_data:
        genres.add(row.genre_name)
        if row.role == 'actor' and row.person_id not in actors:
            actors[row.person_id] = row.full_name
        elif row.role == 'writer' and row.person_id not in writers:
            writers[row.person_id] = row.full_name
        elif row.role == 'director' and row.person_id not in directors:
            directors[row.person_id] = row.full_name
    film = film_data[0]
    es_data = ESFilmworkData(
        id=film_id, imdb_rating=film.rating, genre=list(genres), title=film.title,
        description=film.description, director=list(directors.values()),
        actors_names=list(actors.values()), writers_names=list(writers.values()),
        actors=[{'id': id, 'name': full_name} for id, full_name in actors.items()],
        writers=[{'id': id, 'name': full_name} for id, full_name in actors.items()],
    )
    return es_data
