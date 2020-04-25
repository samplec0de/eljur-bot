from datetime import datetime, timedelta
from typing import Dict, Any

from CTEStorage import cte
from constants import MessageFolder
from utility import load_date, days_equal, format_user


def present_messages(chat_id: int, msgs: Dict[str, Any], folder: str) -> str:
    """
    Генерирует пользовательское отображение списка сообщений
    :param chat_id: идентификатор чата (Telegram, etc)
    :param folder: папка сообщений
    :param msgs: словарь результата getMessages для одной страницы, содержащий обязательно список messages
    :return: отображение сообщений для страницы
    """
    result = ''
    ind = 1
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    for msg in msgs['messages']:
        date = load_date(msg['date'])
        when = f"{date.strftime('%-d %B %H:%M')}"
        if days_equal(date, today):
            when = f"сегодня в {date.strftime('%H:%M')}"
        if days_equal(date, yesterday):
            when = f"вчера в {date.strftime('%H:%M')}"
        if folder == MessageFolder.INBOX:
            user_preview = format_user(msg['user_from'])
        else:
            if 'user_to' not in msg:
                msg['user_to'] = [msg['user_from']]
            user_preview = format_user(msg['user_to'][0])
        files = ' 📎 ' if msg['with_files'] else ''
        unread = ' 🆕 ' if msg['unread'] else ''
        chain = cte.get_cte(chat_id=chat_id).messages_chain(msg_id=msg['id'], folder=folder)
        pos_in_chain = 0
        for chain_msg in chain:
            if chain_msg['id'] == msg['id']:
                break
            pos_in_chain += 1
        answered = ' ✔️ ' if pos_in_chain != 0 \
                             and chain[pos_in_chain - 1]['folder'] == MessageFolder.SENT \
                             and folder == MessageFolder.INBOX else ''
        if msg['unread']:
            result += f"{ind}. <b>{answered}{unread}{files} {user_preview} ({when})</b>\n" \
                      f"<pre>    {msg['subject']}</pre>\n"
        else:
            result += f"{ind}. {answered}{files}<b><i>{user_preview}</i></b> ({when})\n" \
                      f"<i>    {msg['subject']}</i>\n"
        ind += 1
    return '\n' + result + '\n'
