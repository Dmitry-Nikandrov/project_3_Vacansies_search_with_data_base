import json
import os

import requests

from data.config import org_id_list


class APIConnector:
    """подключается с hh.ru и получает необходимую информацию"""

    def __init__(self):
        self.params_vac = {
            "page": 0,
            "per_page": 100,
            "text": "",
            "only_with_salary": True,
        }

    def __get_hh_vacancies(self, companies=org_id_list):
        """подключается с hh.ru и получает информацию о вакансиях по выбранным компаниям"""
        path = "https://api.hh.ru/vacancies"
        list_vacancies = []
        for i in companies:
            self.params_vac["employer_id"] = i
            response_vac = requests.get(path, params=self.params_vac)
            if response_vac.status_code == 200:
                response_vac_json = response_vac.json()["items"]
                list_vacancies.append(response_vac_json)
            else:
                print("Не удалось подключить к ресурсу")
        return list_vacancies

    @property
    def get_hh_vacancies(self):
        return self.__get_hh_vacancies()

    def save_vacancies_in_file(self, filename="data_vacancies.json"):
        """записывает данные о вакансиях в json файл"""
        data_list = self.get_hh_vacancies
        path_2 = os.path.abspath("./data")
        with open(os.path.join(path_2, filename), "w", encoding="utf-8") as file:
            file.write(json.dumps(data_list, ensure_ascii=False, indent=4))
        return f"Создан файл {filename}"

    def __get_hh_employers(self, companies=org_id_list):
        """устанавливает соединение с hh.ru и получает информацию о вакансиях по выбранным компаниям"""
        path = "https://api.hh.ru/employers"
        list_org = []
        for i in companies:
            response = requests.get(f"{path}/{i}")
            if response.status_code == 200:
                response_emp = response.json()
                list_org.append(response_emp)
            else:
                print("Не удалось подключить к ресурсу")
        return list_org

    @property
    def get_hh_employers(self):
        return self.__get_hh_employers()

    def save_employers_in_file(self, filename="data_employers.json"):
        """записывает данные о работодателях в json файл"""
        data_list = self.get_hh_employers
        path_2 = os.path.abspath("./data")
        with open(os.path.join(path_2, filename), "w", encoding="utf-8") as file:
            file.write(json.dumps(data_list, ensure_ascii=False, indent=4))
        return f"Создан файл {filename}"
