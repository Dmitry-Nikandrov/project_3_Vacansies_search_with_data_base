import os

import psycopg2
from dotenv import load_dotenv

from data.config import psycopg_params

load_dotenv()
database = os.getenv('database_name')
employers = os.getenv('db_employers_name')
vacancies = os.getenv('db_vacancies_name')


class DBCreator:
    """создает базы данных postgresql и заполняет их данными о вакансиях и работодателях с hh.ru"""
    def __init__(self):
        pass

    def create_new_db(self, params=psycopg_params):
        """создает базу данных с информацией c hh.ru"""
        conn = psycopg2.connect(dbname='postgres', **params)
        cur = conn.cursor()
        conn.autocommit = True
        cur.execute(f"DROP DATABASE IF EXISTS {database}")
        cur.execute(f'CREATE DATABASE {database}')
        cur.close()
        conn.close()
        return f"Создана база данных {database}"

    def create_databases(self, db1=employers, db2=vacancies, params=psycopg_params):
        """создает базы данных о вакансиях и работодателях в postgresql"""
        with psycopg2.connect(dbname='head_hunter', **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP table IF EXISTS {employers} cascade")
                cur.execute(
                    f"""
                                CREATE table {employers}(
                                            employer_id int primary key,
                                            employer_name varchar(100),
                                            employer_type varchar(50),
                                            description text,
                                            site_url varchar(50)
                                            )"""
                )
                cur.execute(f"DROP table IF EXISTS {vacancies}")
                cur.execute(
                    f"""
                                CREATE table {vacancies}(
                                            id int primary key,
                                            name varchar(100),
                                            employer_id int,
                                            type varchar(100),
                                            salary int,
                                            snippet varchar(255),
                                            area varchar(100),
                                            url varchar(150)
                                            )"""
                )
                cur.execute(f"""
                ALTER TABLE {vacancies} add constraint fk_vacancies_employer_id FOREIGN KEY(employer_id)
                REFERENCES {employers}(employer_id)"""
                            )
        conn.close()
        return "Созданы базы данных vacancies и employers"

    def paste_db_employers(self, total_employers, params=psycopg_params):
        """заполняет ранее созданную бд о работодателях данными с hh.ru"""
        with psycopg2.connect(dbname='head_hunter', **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for i in total_employers:
                    dict_pars = {
                        "employer_id": i["id"],
                        "employer_name": i["name"],
                        "employer_type": i["type"],
                        "description": i["description"],
                        "site_url": i["site_url"],
                    }
                    cur.execute(
                        f"""
                        insert into employers {str(tuple(dict_pars.keys())).replace("'", "")}
                                values ({' ,'.join(['%s']*len(dict_pars))})""",
                        tuple(dict_pars.values()),
                    )
        conn.close()
        return f"Внесены данные в базу данных {employers}"

    def paste_db_vacancies(self, total_vacancies, params=psycopg_params):
        """заполняет ранее созданную бд о вакансиях данными с hh.ru"""
        with psycopg2.connect(dbname='head_hunter', **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                for each_group in total_vacancies:
                    for i in each_group:
                        dict_pars = {
                            "id": i["id"],
                            "name": i["name"],
                            "employer_id": i["employer"]["id"],
                            "type": i["type"]["name"],
                            "salary": i["salary"]["from"],
                            "snippet": i["snippet"]["requirement"],
                            "area": i["area"]["name"],
                            "url": i["url"],
                        }
                        cur.execute(
                            f"""
                            insert into vacancies {str(tuple(dict_pars.keys())).replace("'", "")}
                                    values ({' ,'.join(['%s']*len(dict_pars))})""",
                            tuple(dict_pars.values()),
                        )
        conn.close()
        return f"Внесены данные в базу данных {vacancies}"
