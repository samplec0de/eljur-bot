from copy import deepcopy
from json import loads
from pprint import pprint
from typing import Dict, Optional, Any, List, Union

from requests import get

from constants import MessageFolder


class Eljur:
    def __init__(self, token: str = None, vendor: str = 'eljur'):
        self.token = token  # Токен пользователя, полученный после авторизации (выдаётся на 3 месяца)
        self.api = 'https://api.eljur.ru/api'  # Адрес API eljur.ru
        self._rdata = {
            'auth_token': self.token,
            'vendor': vendor,  # Домен школы
            'out_format': 'json',  # Формат, в котором API eljur.ru будет возвращать данные (есть ещё xml)
            'devkey': '9235e26e80ac2c509c48fe62db23642c',  # Ключ разработчика,
            # запасной: 19c4bfc2705023fe080ce94ace26aec9
        }

    def set_vendor(self, vendor: str) -> None:
        self._rdata['vendor'] = vendor

    def _parse_schedule_like(self, api_path: str) -> Optional[Dict[str, dict]]:
        """
        Получения расписания и домашнего задания, исправляет даты вида ггггммдд в дд.мм.гггг
        """
        r = get(f'{self.api}/{api_path}', params=self._rdata)
        data = loads(r.text)
        if data['response']['state'] != 200:
            return None
        schedule = list(data['response']['result']['students'].values())[0]['days']
        schedule_fixed = dict()
        for key in schedule.keys():
            year = key[:4]
            month = key[4:6]
            day = key[6:]
            date = [day, month, year]
            schedule_fixed['.'.join(date)] = schedule[key]
        return schedule_fixed

    def schedule(self) -> Optional[Dict[str, dict]]:
        """
        Получение расписания уроков
        """
        return self._parse_schedule_like(api_path='getschedule')

    def homework(self) -> Optional[Dict[str, dict]]:
        """
        Получение домашнего задания
        """
        res = self._parse_schedule_like(api_path='gethomework')
        return res

    def get_messages(self, folder: str = MessageFolder.INBOX,
                     page: int = 1,
                     limit: int = 6,
                     unreadonly: bool = False) -> Optional[Dict[str, Any]]:
        """
        Делает запрос к API eljur для получения сообщений пользователя
        :param folder: папка сооьщений (INBOX/SENT)
        :param page: номер страницы
        :param limit: лимит количества сообщений на странице
        :param unreadonly: возвращать только непрочитанные
        :return: словарь с полями limit, page, messages
        """
        params = deepcopy(self._rdata)
        params['folder'] = str(folder)
        if unreadonly:
            params['unreadonly'] = str(True).lower()
        params['limit'] = str(limit)
        params['page'] = str(page)
        request = get(f'{self.api}/getmessages', params=params)
        if request.status_code != 200:
            return None
        return loads(request.text)['response']['result']

    def get_message(self, msg_id: str) -> Dict[str, Any]:
        """
        Делает запрос к API eljur для получения сообщения с id msg_id
        :param msg_id:
        :return:
        """
        params = deepcopy(self._rdata)
        params['id'] = msg_id
        request = get(f'{self.api}/getmessageinfo', params=params)
        if request.status_code != 200:
            return {}
        msg = loads(request.text)['response']['result']['message']
        msg['with_files'] = 'files' in msg and len(msg['files']) > 0
        return msg

    def message_receivers(self, group: Optional[str] = None) \
            -> Optional[Dict[str, List[Dict[str, Union[List[Dict[str, str]], str]]]]]:
        """
        Отдаёт возможных получателей сообщения группами или получателей из заданной группы
        :param group: название группы пользователей из поля key
        :return: список групп или пользователей группы
        """
        request = get(f'{self.api}/getmessagereceivers', params=self._rdata)
        if request.status_code != 200:
            return None
        if group:
            return loads(request.text)['response']['result']['groups'][group]
        return loads(request.text)['response']['result']['groups']

    def send_message(self, users_to: str, subject: str, text: str) -> bool:
        """
        Отправляет сообщение пользователям
        :param users_to: список пользователей через запятую
        :param subject: тема сообщения
        :param text: текст сообщения
        :return: результат отправки - успех/ошибка
        """
        params = deepcopy(self._rdata)
        params['subject'] = subject
        params['text'] = text
        params['users_to'] = users_to
        request = get(f'{self.api}/sendmessage', params=params)
        if request.status_code != 200:
            return False
        return True

    def reply_message(self, replyto: str, text: str) -> bool:
        """
        Отправляет ответ на сообщение
        :param replyto: id сообщения для ответа
        :param text: текст сообщения
        :return: результат отправки - успех/ошибка
        """
        params = deepcopy(self._rdata)
        params['replyto'] = replyto
        params['text'] = text
        request = get(f'{self.api}/sendreplymessage', params=params)
        if request.status_code != 200:
            return False
        return True

    def profile(self) -> Optional[dict]:
        """
        Основная информация о пользователе
        """
        request = get(f'{self.api}/getrules', params=self._rdata)
        if request.status_code != 200:
            return None
        return loads(request.text)['response']['result']

    def periods(self, show_disabled: bool = True) -> Optional[List[Dict[str, Optional[str]]]]:
        """
        Учебные периоды пользователя
        :param show_disabled: возвращать ли ещё не наступившие периоды
        """
        request = get(f'{self.api}/getperiods', params={**self._rdata, 'show_disabled': show_disabled})
        if request.status_code != 200:
            return None
        return loads(request.text)['response']['result']['students'][0]['periods']

    def marks(self, last_period: bool = True) -> Optional[Dict[str, Any]]:
        period = None
        if last_period:
            periods = self.periods(show_disabled=False)
            if periods:
                period = f"{periods[-1]['start']}-{periods[-1]['end']}"
        request = get(f'{self.api}/getmarks', params={**self._rdata, 'days': period})
        if request.status_code != 200:
            return None
        return list(loads(request.text)['response']['result']['students'].values())[0]
