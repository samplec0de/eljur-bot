import hashlib
import re
from copy import deepcopy
from datetime import datetime
from typing import Dict

from constants import MessageFolder

cleaner = re.compile('<.*?>')


def clean_html(raw_html: str) -> str:
    """
    Очищает строку от html-тэгов
    :param raw_html: строка, содержащая html
    :return: строка, очищенная от html
    """
    return re.sub(cleaner, '', raw_html)


def load_date(date_string: str, fmt: str = '%Y-%m-%d %H:%M:%S') -> datetime:
    """
    Конвертирует строку заданного формата в объект datetime
    :param date_string: строка с датой по формату fmt
    :param fmt: формат даты в строке
    :return: datetime представление строки
    """
    return datetime.strptime(date_string, fmt)


def days_equal(date1: datetime, date2: datetime) -> bool:
    """
    Проверяет, что две даты совпадают по году, месяцу и дню
    :param date1: первая дата
    :param date2: вторая дата
    :return: совпадают ли даты
    """
    return date1.day == date2.day and date1.month == date2.month and date1.year == date2.year


def hash_string(text: str) -> str:
    """
    SHA-256 для переданной строки
    :param text: исходная строка
    :return: SHA-256 строки
    """
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def format_user(info: Dict[str, str], fmt: str = '{lastname} {firstname_short}.{middlename_short}') -> str:
    """
    Преобразовывает словарь eljur-данных пользователя в строку по заданному формату
    :param info: словарь eljur-данных, обязательно содержащий ключи firstname, lastname, middlename
    :param fmt: формат, содержащий дрпустимые переменные:
    firstname, lastname, middlename, firstname_short, middlename_short, lastname_short
    :return:
    """
    uinfo = deepcopy(info)
    uinfo['firstname_short'] = uinfo['firstname'][0] if uinfo['firstname'] else ''
    uinfo['lastname_short'] = uinfo['lastname'][0] if uinfo['lastname'] else ''
    uinfo['middlename_short'] = uinfo['middlename'][0] if uinfo['middlename'] else ''
    return fmt.format(**uinfo)


def folder_to_string(folder: str):
    """
    Текстовая интерпретация типа папки сообщений
    :param folder: inbox/sent
    :return: Входящие/отправленные
    """
    return folder.replace('inbox', 'Входящие').replace('sent', 'Отправленные')


def opposite_folder(folder: str):
    """
    Противоположная папка (для inbox - sent, для sent - inbox)
    :param folder: папка, для которой нужно определить противоположную
    :return: inbox/sent
    """
    if folder == MessageFolder.INBOX:
        return MessageFolder.SENT
    return MessageFolder.INBOX
