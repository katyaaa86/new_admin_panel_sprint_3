from datetime import datetime
from typing import Optional

from pydantic import BaseModel, BaseSettings, Field


class PostgresSettings(BaseSettings):
    dbname: str = Field('movies_database', env='DB_NAME')
    user: str = Field('app', env='DB_USER')
    password: str = Field('123qwe', env='DB_PASSWORD')
    host: str = Field('127.0.0.1', env='DB_HOST')
    port: int = Field(5434, env='DB_PORT')


class ElasticSettings(BaseSettings):
    es_host: str = Field('http://127.0.0.1:9200', env='ES_HOST')
    es_user: str = Field('', env='ES_USER')
    es_password: str = Field('', env='ES_PASSWORD')


class FilmworkData(BaseModel):
    fw_id: str
    title: str
    description: Optional[str]
    rating: Optional[float]
    type: str
    created: datetime
    modified: datetime
    role: Optional[str]
    person_id: Optional[str]
    full_name: Optional[str]
    genre_name: str


class ESPersonData(BaseModel):
    id: str
    name: str


class ESFilmworkData(BaseModel):
    id: str
    imdb_rating: Optional[float]
    genre: list[str]
    title: str
    description: Optional[str]
    director: list[str]
    actors_names: list[str]
    writers_names: list[str]
    actors: list[ESPersonData]
    writers: list[ESPersonData]
