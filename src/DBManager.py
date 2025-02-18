import psycopg2

from data.config import psycopg_params


class DBManager:
    """взаимодействует с базой данных и получая необходимую информацию"""

    def __init__(self):
        pass

    def __get_companies_and_vacancies_count(self, params=psycopg_params):
        """получает список всех компаний и количество вакансий у каждой компании"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                            select employers.employer_name, count(name) as quantity from vacancies
                            join employers using(employer_id)
                            group by employers.employer_name
                            """
                )
                rows = cur.fetchall()
                print("\nCписок всех компаний и количество вакансий у каждой компании:")
                for row in rows:
                    print(row)
        conn.close()
        return ""

    @property
    def get_companies_and_vacancies_count(self):
        return self.__get_companies_and_vacancies_count()

    def __get_all_vacancies(self, params=psycopg_params):
        """получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                            select employers.employer_name, vacancies.name, salary, url from vacancies
                            join employers using(employer_id)
                            """
                )
                rows = cur.fetchall()
                print("\nСписок всех вакансий с выбранными атрибутами:")
                for row in rows:
                    print(row)
        conn.close()
        return ""

    @property
    def get_all_vacancies(self):
        return self.__get_all_vacancies()

    def __get_avg_salary(self, params=psycopg_params):
        """получает среднюю зарплату по вакансиям"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""
                select avg(salary) as average_wage from vacancies
                """
                            )
                rows = cur.fetchall()
                print("\nСредняя зарплата по всем вакансиям:")
                for row in rows:
                    print(round(float(*row), 2), "руб.")
        conn.close()
        return ""

    @property
    def get_avg_salary(self):
        return self.__get_avg_salary()

    def __get_vacancies_with_higher_salary(self, params=psycopg_params):
        """получает список всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    """
                            select vacancies.name, employers.employer_name, salary from vacancies
                            join employers using(employer_id)
                            where salary > (select avg(salary) from vacancies)
                            """
                )
                rows = cur.fetchall()
                print(
                    "\nСписок всех вакансий с зарплатой выше средней по всем вакансиям:"
                )
                for row in rows:
                    print(row)
        conn.close()
        return ""

    @property
    def get_vacancies_with_higher_salary(self):
        return self.__get_vacancies_with_higher_salary()

    def get_vacancies_with_keyword(self, keyword="менедж", params=psycopg_params):
        """получает список всех вакансий, в названии которых содержатся переданные в метод слова"""
        with psycopg2.connect(dbname="postgres", **params) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    select vacancies.name, employers.employer_name, salary from vacancies
                    join employers using(employer_id) where vacancies.name like '%{keyword}%'
                    """
                )
                rows = cur.fetchall()
                print(
                    "\nСписок всех вакансий с заданным сочетанием букв в названии вакансии:"
                )
                for row in rows:
                    print(row)
        conn.close()
        return ""
