from src.API_connect import APIConnector
from src.DBCreater import DBCreator
from src.DBManager import DBManager


"получение данных с hh.ru их преобразование в объект питона и запись в json файл"
APIConnector = APIConnector()
get_hh_vacancies = APIConnector.get_hh_vacancies()
APIConnector.save_vacancies_in_file()
get_hh_emloyers = APIConnector.get_hh_employers()
APIConnector.save_employers_in_file()

"создание баз данных о вакансиях и работодателях в postgresql с последующим заполнением данными с hh.ru"
DBCreator = DBCreator()
DBCreator.create_databases()
DBCreator.paste_db_employers(get_hh_emloyers)
DBCreator.paste_db_vacancies(get_hh_vacancies)

"взаимодействие с базами данных о вакансиях и работодателях в postgresql. получение аналитических сведений"
DBManager = DBManager()
DBManager.get_companies_and_vacancies_count()
DBManager.get_all_vacancies()
DBManager.get_avg_salary()
DBManager.get_vacancies_with_higher_salary()
DBManager.get_vacancies_with_keyword()
