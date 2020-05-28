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
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    for ind, msg in enumerate(msgs['messages']):
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
                if 'user_from' in msg:
                    msg['user_to'] = [msg['user_from']]
                else:
                    msg['user_to'] = msg['users_to']
            user_preview = format_user(msg['user_to'][0])
            if 'users_to' in msg and len(msg['users_to']) > 1:
                user_preview += f" и ещё {len(msg['users_to']) - 1}"
        files = ' 📎 ' if msg['with_files'] else ''
        chain = cte.get_cte(chat_id=chat_id).messages_chain(msg_id=msg['id'], folder=folder)
        chain_pos = 0
        for chain_msg in chain:
            if chain_msg['id'] == msg['id']:
                break
            chain_pos += 1
        if chain_pos != 0 and chain[chain_pos - 1]['folder'] == MessageFolder.SENT and folder == MessageFolder.INBOX:
            answered = ' ↪️️ '
        else:
            answered = ''
        if msg['unread']:
            result += f"{ind + 1}. <b>{answered}{files} {user_preview} ({when})</b>\n" \
                      f"<pre>    {msg['subject']}</pre>\n"
        else:
            result += f"{ind + 1}. {answered}{files}<i>{user_preview}</i> ({when})\n" \
                      f"<i>    {msg['subject']}</i>\n"
        ind += 1
    return '\n' + result + '\n'
