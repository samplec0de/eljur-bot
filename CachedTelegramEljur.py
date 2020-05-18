import logging
import os
import time
from base64 import b64encode
from concurrent.futures.thread import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from json import loads
from threading import Lock, Thread
from typing import Optional, List, Union, Dict, Any

import pymongo
from pymongo.errors import BulkWriteError
from requests import post

from constants import *
from eljur import Eljur
from utility import load_date, hash_string

mongo = pymongo.MongoClient(os.environ.get('mongo_uri'))
db = mongo[os.environ['database']]
messages = db['messages']
cache_queue = db['cache_queue']
messages.create_index([("hash", pymongo.DESCENDING)], unique=True)
data = db['data']
homework = db['homework']
logger = logging.getLogger('CachedTelegramEljur')


class CachedTelegramEljur(Eljur):
    download_in_progress: bool  # индикатор того, что сообщения уже кэшируются
    msgs_load_limit: int  # Лимит по количеству сообщений для первичной загрузки в память, автоматически расширяется
    token: Optional[str]  # eljur токен пользователя
    chat_id: int  # идентификатор чата (Telegram, etc)
    cached_message_ids: Dict[str, List[str]]  # добавленные в кэш сообщения
    not_cached: List[dict]  # сообщения, которые предстоит добавить в кэш
    msg_cache: Dict[str, List[dict]]  # кэш сообщений в памяти
    user_info: Optional[Dict[str, str]]  # ФИО пользователя {name: id, firstname: a, middlename: b, lastname: c}

    def __init__(self, chat_id: int, no_messages: bool = False):
        super().__init__()
        self.chat_id = chat_id
        self.token = self.auth_token
        if self.vendor:
            super().__init__(self.token, self.vendor)
        else:
            super().__init__(self.token)
        self.msg_cache = {MessageFolder.INBOX: [], MessageFolder.SENT: []}
        self.msgs_load_limit = MESSAGES_PER_USER_ML
        self.not_cached = []
        for item in cache_queue.find({'chat_id': self.chat_id}):
            item.pop('_id', None)
            self.not_cached.append(item)
        self.cached_message_ids = {MessageFolder.INBOX: [], MessageFolder.SENT: []}
        self.download_in_progress = False
        self.user_info = {
            'firstname': self.user_data('firstname'),
            'lastname': self.user_data('lastname'),
            'middlename': self.user_data('middlename'),
            'name': self.user_data('name'),
        }
        if not self.user_info['firstname']:
            self.user_info = None
        if not no_messages:
            for folder_type in FOLDER_TYPES:
                self.msg_cache[folder_type] = self.messages(folder=folder_type)
                if not self.msg_cache[folder_type]:
                    self.download_messages_preview(check_new_only=False, folder=folder_type)
        self._lock = Lock()

    def user_data(self, field: str) -> Any:
        """
        Позволяет получить информацию о пользователи из коллекции "data"
        :param field: имя поля, значение которого нужно получить из базы данных
        :return: значение поля или None, если оно не задано для пользователя
        """
        document = data.find_one({'chat_id': self.chat_id})
        if document and field in document:
            return document[field]
        return None

    @property
    def vendor(self):
        res = data.find_one({'chat_id': self.chat_id})
        if res and 'vendor' in res:
            return res['vendor']
        return None

    @property
    def auth_token(self) -> Optional[str]:
        """
        Позволяет получить токен eljur из базы
        :return: токен доступа eljur
        """
        if not self.token:
            document = data.find_one({'chat_id': self.chat_id})
            if document:
                self.token = document['auth_token']
            return self.token

    @auth_token.setter
    def auth_token(self, token: str) -> None:
        """
        Устанавливает токен eljur пользователя
        :param token: валидный токен eljur
        """
        self.token = token
        super().__init__(token)
        document = data.find_one({'chat_id': self.chat_id})
        if document:
            data.find_one_and_update({'chat_id': self.chat_id}, {'$set': {'auth_token': token}})
        else:
            data.insert_one({'chat_id': self.chat_id, 'auth_token': token})

    @property
    def token_expire(self) -> Optional[datetime]:
        """
        Позволяет определить дату, до которой токен активен
        :return: дата, до которой токен eljur активен
        """
        document = data.find_one({'chat_id': self.chat_id})
        if document:
            return document['token_expire']
        return None

    @token_expire.setter
    def token_expire(self, expire: datetime) -> None:
        """
        Устанавливает дату, до которой токен активен
        :param expire: дата, до которой токен eljur будет работать
        """
        document = data.find_one({'chat_id': self.chat_id})
        if document:
            data.find_one_and_update({'chat_id': self.chat_id}, {'$set': {'token_expire': expire}})
        else:
            data.insert_one({'chat_id': self.chat_id, 'token_expire': expire})

    def auth(self, login: str, password: str, vendor: str = 'eljur') -> bool:
        """
        Авторизовывает пользователя по логину и паролю
        """
        r = post('https://api.eljur.ru/api/auth', data={
            'login': login,
            'password': password,
            'vendor': vendor,
            'devkey': '9235e26e80ac2c509c48fe62db23642c',  # 19c4bfc2705023fe080ce94ace26aec9
            'out_format': 'json'
        })
        if r.status_code == 200:
            tdata = loads(r.text)['response']['result']
            self.auth_token = tdata['token']
            self.token_expire = load_date(tdata['expires'])
            data.find_one_and_update(
                {
                    'chat_id': self.chat_id
                },
                {
                    '$set': {
                        'login': login,
                        **Eljur(token=tdata['token'], vendor=vendor).profile(),
                        'password': b64encode(bytes(password, encoding='utf-8'))
                        # TODO: реализовать переавторизацию
                    }
                }
            )
            self.user_info = {
                'firstname': self.user_data('firstname'),
                'lastname': self.user_data('lastname'),
                'middlename': self.user_data('middlename'),
                'name': self.user_data('name'),
            }
            return True
        return False

    def messages(self, folder: str) -> List[dict]:
        """
        Загружает необходимое (load_limit) количество сообщений из памяти или базы данных
        :param folder: папка (sent/inbox)
        :return: необходимое количество сообщений папки folder
        """
        logger.debug(f'Подгружаю сообщения из базы для {self.chat_id}')
        not_cached = list()
        if not self.msg_cache[folder] or len(self.msg_cache[folder]) < self.msgs_load_limit:
            logger.debug(f'Аннулирование кэша для {self.chat_id}')
            self.msg_cache[folder].clear()
            for item in messages.find(
                    {'chat_id': self.chat_id, 'folder': folder}
            ).sort('date', pymongo.DESCENDING).limit(self.msgs_load_limit):
                self.msg_cache[folder].append(item)
                if not item['unread'] and 'text' not in item:
                    not_cached.append({'chat_id': self.chat_id, 'folder': item['folder'], 'id': item['id']})
        if not_cached:
            for item in not_cached:
                cache_queue.update_one(item, {'$set': item}, upsert=True)
            self.not_cached.extend([item for item in not_cached if item not in self.not_cached])
        return self.msg_cache[folder]

    def messages_count(self, folder: str) -> int:
        """
        Позволяет получить количество сообщений в папке folder в элжуре
        :param folder: папка (sent/inbox)
        :return: реальное количество сообщений из базыы
        """
        res = data.find_one({'chat_id': self.chat_id})
        count_key = f'messages_count_{folder}'
        if count_key not in res:
            total_from_api = int(super().get_messages(folder=folder)['total'])
            data.find_one_and_update({'chat_id': self.chat_id}, {'$set': {count_key: total_from_api}})
            return total_from_api
        return res[count_key]

    def set_messages_count(self, folder: MessageFolder, count: int) -> int:
        """
        Устанавливает реальное количество сообщений в папке folder в элжуре
        :param folder: папка (sent/inbox)
        :param count: количество сообщений (получается через API)
        :return: реальное количество сообщений, записанное в базу
        """
        count_key = f'messages_count_{folder}'
        res = data.find_one_and_update({'chat_id': self.chat_id}, {'$set': {count_key: count}})
        return res[count_key]

    def add_one_message(self, folder: str, msg_data: dict):
        """
        Добавляет одно сообщение с содержимым msg_data в папку folder (inbox/sent)
        """
        messages.insert_one({'chat_id': self.chat_id,
                             'folder': folder,
                             'hash': hash_string(f'{self.chat_id}_{folder}_{msg_data["id"]}'),
                             **msg_data})

    def mark_as_read(self, folder: str, msg_id: str):
        """
        Отмечает сообщение как прочитанное в кэше и базе
        """
        for msg in self.msg_cache[folder]:
            if msg['id'] == msg_id:
                msg['unread'] = False
                break
        messages.find_one_and_update({'chat_id': self.chat_id, 'id': msg_id}, {'$set': {'unread': False}})

    def get_message(self, msg_id: str,
                    only_cache: bool = False,
                    force_folder: Optional[str] = None, no_eljur_request: bool = False) -> Union[Optional[dict], str]:
        """
        Пытается найти полную версию сообщения в базе
        """
        if type(msg_id) == tuple:
            msg_id, only_cache = msg_id
        document = messages.find_one({'chat_id': self.chat_id, 'id': msg_id,
                                      'folder': force_folder if force_folder else MessageFolder.INBOX})
        if document and 'text' in document:
            if only_cache:
                return msg_id
            if not no_eljur_request and document['unread']:  # Прочтение сообщения на стороне eljur
                Thread(target=super().get_message, args=[msg_id], daemon=True).start()
            return document
        if only_cache and document and document['unread']:
            logger.debug(f'{msg_id} не будет сохраняться сейчас, потому что оно ещё не прочтено')
            return msg_id
        if no_eljur_request:
            return document
        msg_data = super().get_message(msg_id=msg_id)
        if not msg_data:
            logging.error(f'Не удалось получить от элжура сообщение с id {msg_id}')
            return None
        if force_folder:
            self._cache_full_message(msg_id=msg_id, msg_data=msg_data, folder=force_folder)
        else:
            self._cache_full_message(msg_id=msg_id, msg_data=msg_data, folder=MessageFolder.INBOX)
            self._cache_full_message(msg_id=msg_id, msg_data=msg_data, folder=MessageFolder.SENT)
        if not only_cache:
            return self.get_message(msg_id=msg_id, force_folder=force_folder)
        return msg_id

    def get_messages(self, folder: str = MessageFolder.INBOX, page: int = 1, limit: int = 6, unreadonly: bool = False) \
            -> Dict[str, Union[str, list, int]]:
        """
        Позволяет получить сообщения из кэша в формате, в котором API eljur возвращает сообщения
        :param folder: папка (sent/inbox)
        :param page: номер страницы сообщений
        :param limit: максимальное количество сообщений на одной странице
        :param unreadonly: если True, возвращает только непрочитанные
        :return: limit или менее сообщений в формате элжура {total: x, count: x, messages: [a, b, c]}
        """
        result = dict()
        offset = limit * (page - 1)
        if len(self.msg_cache[folder]) < offset + limit:  # Требуется дозагрузка сообщений
            self.msgs_load_limit = offset + limit + 1
            logger.debug(f'Лимит для {self.chat_id} изменен на {self.msgs_load_limit}')
            msgs = deepcopy(self.messages(folder=folder))
        else:
            msgs = deepcopy(self.msg_cache[folder])
        if unreadonly:
            msgs = [msg for msg in msgs if msg['unread']]
        msgs = [msg for msg in msgs if msg['folder'] == folder]
        result['total'] = self.messages_count(folder=folder)
        result['messages'] = msgs[offset:offset + limit]
        result['count'] = len(result['messages'])
        return result

    def unread_count(self, folder: str = MessageFolder.INBOX):
        """
        Количество непрочитанных сообщений
        :return: количество непрочитанных сообщений пользователя
        """
        return messages.count({'chat_id': self.chat_id, 'folder': folder, 'unread': True})

    def _cache_full_message(self, msg_id: str, folder: str, msg_data: dict) -> None:
        """
        Добавляет сообщение в кэш
        """
        target = {'chat_id': self.chat_id, 'id': msg_id, 'folder': folder}
        if messages.find_one(target):
            messages.find_one_and_update(target, {'$set': msg_data})
            cache_queue.delete_one(target)
            with self._lock:
                if target in self.not_cached:
                    self.not_cached.remove(target)
                else:
                    logger.info(f'Кэширую сообщение {msg_id} по запросу пользователя {self.chat_id}')
        else:
            logger.info(f'Сообщение с id {msg_id} в {folder} НЕ ДОБАВЛЕНО в кэш для {self.chat_id} (не найдено в бд)')

    def cache_full_messages(self):
        """
        Кэширует полные сообщения пользователя (такие поля как текст и др.)
        """
        logger.info(f'Работа по кэшированию сообщений для {self.chat_id} начата, осталось {len(self.not_cached)}')
        with ThreadPoolExecutor(max_workers=MESSAGES_CACHE_THREADS) as pool:
            for msg_id in pool.map(lambda p: self.get_message(msg_id=p['id'],
                                                              force_folder=p['folder'],
                                                              only_cache=True),
                                   self.not_cached):
                logger.info(f"Сообщение для {self.chat_id} с id {msg_id} добавлено в базу")

    def message_ids(self, folder: str) -> List[str]:
        """
        Возвращает список id сообщений из базы или подгружает их, если в базе пусто
        """
        if self.cached_message_ids[folder]:
            return self.cached_message_ids[folder]
        messages_key = f'messages_{folder}'
        res = data.find_one({'chat_id': self.chat_id})
        if res and messages_key in res and len(res[messages_key]) > 0:
            self.cached_message_ids[folder] = res[messages_key]
            return self.cached_message_ids[folder]
        ids = set()
        for msg in messages.find({'chat_id': self.chat_id, 'folder': folder}):
            ids.add(msg['id'])
        data.find_one_and_update({'chat_id': self.chat_id}, {'$set': {messages_key: list(ids)}})
        return list(ids)

    def add_message_ids(self, folder: str, ids: List[str]) -> None:
        messages_key = f'messages_{folder}'
        self.cached_message_ids[folder] = ids + self.cached_message_ids[folder]
        data.find_one_and_update({'chat_id': self.chat_id},
                                 {'$set': {messages_key: self.cached_message_ids[folder]}})

    def message_exist(self, folder: str, msg_id: str):
        """
        Проверяет существует ли в базе сообщение с id msg_id в папке folder
        """
        res = messages.find_one({'hash': hash_string(f'{self.chat_id}_{folder}_{msg_id}')})
        if res:
            return True
        return False

    def download_messages_preview(self, check_new_only: bool, folder: str, limit: int = 1000) -> List[dict]:
        """
        Обновляет кэш сообщений и возвращает список новых входящих сообщений
        """
        if self.download_in_progress:
            return []
        self.download_in_progress = True
        new_messages = []
        page_to = MAX_CACHE_PAGES - 1 if limit == 1000 else 1
        for msg_type in FOLDER_TYPES:
            for page in range(1, page_to + 1):
                msgs = super().get_messages(folder=msg_type, page=page, limit=limit)
                if not msgs or 'messages' not in msgs:
                    break
                new_messages.extend([{'chat_id': self.chat_id,
                                      'folder': msg_type,
                                      'hash': hash_string(f'{self.chat_id}_{msg_type}_{msg["id"]}'), **msg}
                                     for msg in msgs['messages']
                                     if not self.message_exist(folder=msg_type, msg_id=msg['id'])])
        self.msg_cache[folder] = new_messages + self.msg_cache[folder]
        not_cached = []
        if not check_new_only:
            for msg in new_messages:
                if not msg['unread']:
                    not_cached.append({'chat_id': self.chat_id, 'folder': msg['folder'], 'id': msg['id']})
        if new_messages:
            try:
                if not_cached:
                    cache_queue.insert_many(deepcopy(not_cached))
                self.not_cached.extend(not_cached)
                messages.insert_many(new_messages)
                # self.add_message_ids(folder=folder, ids=[msg['id'] for msg in new_messages])
            except BulkWriteError as bwe:
                logger.error(f'[0] BulkWriteError:\n{bwe.details}')
        self.download_in_progress = False
        return [msg for msg in new_messages if msg['folder'] == MessageFolder.INBOX]

    def messages_chain(self, msg_id: str, folder: str) -> List[Dict[str, Any]]:
        """
        Позволяет получить цепочку сообщений, содержащую msg_id
        """
        src_msg = self.get_message(msg_id=msg_id, force_folder=folder, no_eljur_request=True)
        subject = src_msg.get('subject')
        reply = subject.startswith('Re: ')
        if reply:
            subject = subject[4:]
        elif folder == MessageFolder.SENT:
            return [src_msg]
        reply_from_me = folder == MessageFolder.SENT
        if reply_from_me:
            if 'users_to' in src_msg:
                he = src_msg['users_to'][0]
            else:
                he = src_msg['user_from']
        else:
            he = src_msg['user_from']
        chain = []
        for msg in messages.find(
                {
                    'chat_id': self.chat_id,
                    'subject': {
                        '$in': [f'Re: {subject}', subject]
                    },
                    '$or': [
                        {'user_from': he},
                        {'users_to': [he]},
                    ]
                }
        ):
            chain.append(msg)
        chain.sort(key=lambda item: load_date(item['date']).timestamp())
        return chain[::-1]

    def reply_message(self, replyto: str, text: str) -> bool:
        """
        Отправляет ответ на сообщение
        :param replyto: id сообщения для ответа
        :param text: текст сообщения
        :return: результат отправки - успех/ошибка
        """
        result = super().reply_message(replyto=replyto, text=text)
        self.download_messages_preview(check_new_only=True, folder=MessageFolder.SENT, limit=1)
        return result

    def update_read_state(self, folder: str) -> None:
        """
        Обновляет статус прочтения сообщений
        """
        result = super().get_messages(folder=folder, limit=1000, unreadonly=True)
        messages.update_many({'chat_id': self.chat_id, 'folder': folder, 'unread': True}, {'$set': {'unread': False}})
        if 'messages' in result:
            ids = [msg['id'] for msg in result['messages']]
            if ids:
                messages.update_many({'chat_id': self.chat_id, 'folder': folder, 'id': {'$in': ids}},
                                     {'$set': {'unread': True}})
        self.msg_cache[folder].clear()
        self.messages(folder=folder)

    @property
    def homework(self) -> Optional[Dict[str, dict]]:
        """
        Позволяет получить домашнее задание из базы или получить с eljur
        """
        homework_data = homework.find_one({'chat_id': self.chat_id})
        last_update = -1
        mode_update = False
        if homework_data:
            last_update = homework_data['last_update']
            mode_update = True
        if not last_update or time.time() - last_update > 60:
            hw = super().homework()
            hw_db = dict()
            for key in list(hw.keys()):
                hw_db[key.replace('.', '-')] = hw[key]
            homework_data = {'last_update': time.time(), 'homework': hw_db}
            if mode_update:
                homework.find_one_and_update({'chat_id': self.chat_id}, {'$set': homework_data})
            else:
                homework.insert_one({'chat_id': self.chat_id, **homework_data})
            return hw
        else:
            hw = homework_data['homework']
            for key in list(hw.keys()):
                hw[key.replace('-', '.')] = hw[key]
                hw.pop(key, None)
            return hw
