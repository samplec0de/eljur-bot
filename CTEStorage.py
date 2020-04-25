from typing import Dict
from CachedTelegramEljur import CachedTelegramEljur


class CTEStorage:
    ctes: Dict[int, CachedTelegramEljur]

    def __init__(self):
        self.ctes = dict()

    def get_cte(self, chat_id: int) -> CachedTelegramEljur:
        """
        Создает или отдает из кеша объект пользователя
        :param chat_id: идентификатор чата
        :return: экземпляр класса пользователя
        """
        if chat_id not in self.ctes:
            self.ctes[chat_id] = CachedTelegramEljur(chat_id=chat_id)
        return self.ctes[chat_id]

    @property
    def cached_chats(self):
        """
        :return: список идентификаторов кэшированных пользователей
        """
        return list(self.ctes.keys())


cte = CTEStorage()
