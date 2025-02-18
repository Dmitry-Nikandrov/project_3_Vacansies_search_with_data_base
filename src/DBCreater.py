import psycopg2

from data.config import psycopg_params


class DBCreator:
    """создает базы данных postgresql и заполняет их данными о вакансиях и работодателях с hh.ru"""
    def __init__(self):
        pass

    def create_databases(self, database_1="employers", database_2="vacancies", params=psycopg_params):
        """создает базы данных о вакансиях и работодателях в postgresql"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"DROP table IF EXISTS {database_1} cascade")
                cur.execute(
                    f"""
                                CREATE table {database_1}(
                                            employer_id int primary key,
                                            employer_name varchar(100),
                                            employer_type varchar(50),
                                            description text,
                                            site_url varchar(50)
                                            )"""
                )
                cur.execute(f"DROP table IF EXISTS {database_2}")
                cur.execute(
                    f"""
                                CREATE table {database_2}(
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
                cur.execute("""
                ALTER TABLE vacancies add constraint fk_vacancies_employer_id FOREIGN KEY(employer_id)
                REFERENCES employers(employer_id)"""
                            )
        conn.close()
        return "Созданы базы данных vacancies и employers"

    def paste_db_employers(self, total_employers, params=psycopg_params):
        """заполняет ранее созданную бд о работодателях данными с hh.ru"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
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
        return "Внесены данные в базу данных employers"

    def paste_db_vacancies(self, total_vacancies, params=psycopg_params):
        """заполняет ранее созданную бд о вакансиях данными с hh.ru"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
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
        return "Внесены данные в базу данных vacancies"
