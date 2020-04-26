import locale
import logging
import math
import os
import time
import traceback
from pathlib import Path
from threading import Thread
from typing import Dict, Union

import pymongo
from pymorphy2 import MorphAnalyzer
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ParseMode, ReplyKeyboardRemove, ReplyKeyboardMarkup, \
    Update
from telegram.ext import Updater, CommandHandler, CallbackQueryHandler, PicklePersistence, \
    ConversationHandler, MessageHandler, Filters, CallbackContext

from CTEStorage import cte
from CachedTelegramEljur import CachedTelegramEljur
from constants import *
from homework import homework_handler, homework
from messages import present_messages
from utility import format_user, opposite_folder, folder_to_string

locale.setlocale(locale.LC_TIME, 'ru_RU.UTF-8')
data_dir = Path(__file__).parent / 'data'
if not data_dir.exists():
    os.mkdir(data_dir)
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO,
                    filename=str(data_dir / 'bot.log'))
logger = logging.getLogger("BOT")
logger_cte = logging.getLogger('CachedTelegramEljur')
logging.getLogger("requests").setLevel(logging.WARNING)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(logging.INFO)
logger_cte.addHandler(ch)
morph = MorphAnalyzer()
LOGIN, WAIT_LOGIN, WAIT_PASSWORD, MAIN_MENU = range(4)
mongo = pymongo.MongoClient(f"mongodb://root:efGel-343ed3-e33e@194.147.78.78:27017/admin")
db = mongo[os.environ['database']]
data = db['data']


def error(update, context):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, context.error)


def start(update: Update, context: CallbackContext):
    """
    Точка входа в бота
    :param update: передается библиотекой телеграма
    :type context: передается библиотекой телеграма
    """
    keyboard = [['Войти в элжур']]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    update.message.reply_text('Здравствуйте! Подключитесь к элжуру:', reply_markup=reply_markup)
    return LOGIN


def send_menu(update: Update, context: CallbackContext):
    """
    Вывод главного меню
    :param update: передается библиотекой телеграма
    :type context: передается библиотекой телеграма
    """
    keyboard = [['Сообщения', 'Домашнее задание']]
    reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    update.message.reply_text('Меню', reply_markup=reply_markup)


def login_handler(update: Update, context: CallbackContext):
    """
    Ввод логина пользователем
    :param update: передается библиотекой телеграма
    :type context: передается библиотекой телеграма
    """
    update.message.reply_text(text='Введите ваш логин:', reply_markup=ReplyKeyboardRemove())
    return WAIT_LOGIN


def user_send_login(update: Update, context: CallbackContext):
    """
    Ввод пароля пользователем
    :param update: передается библиотекой телеграма
    :type context: передается библиотекой телеграма
    """
    context.user_data['eljur_login'] = update.message.text
    update.message.reply_text(text='Введите пароль:')
    return WAIT_PASSWORD


def user_send_password(update: Update, context: CallbackContext):
    ejuser = CachedTelegramEljur(chat_id=update.message.chat.id, no_messages=True)
    if ejuser.auth(login=context.user_data['eljur_login'], password=update.message.text):
        update.message.reply_text('Вы успешно вошли в элжур! Выполняю синхронизацию, пожалуйста, подождите.')
        cte.get_cte(chat_id=update.message.chat.id)  # Кэшируем сообщения
        if update.message.chat.id not in authorized_chat_ids:
            authorized_chat_ids.append(update.message.chat.id)
        updater.job_queue.run_repeating(check_for_new_messages,
                                        interval=MESSAGES_CHECK_DELAY,
                                        first=MESSAGES_CHECK_DELAY,
                                        context=update.message.chat.id)
        send_menu(update=update, context=context)
        return MAIN_MENU
    else:
        keyboard = [['Попробовать ещё раз']]
        reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        update.message.reply_text(f'Неправильный логин/пароль', reply_markup=reply_markup)
        return ConversationHandler.END


def cancel(update: Update, context: CallbackContext):
    user = update.message.from_user
    logger.info("User %s canceled the conversation.", user.first_name)
    update.message.reply_text('Bye! I hope we can talk again some day.',
                              reply_markup=ReplyKeyboardRemove())

    return ConversationHandler.END


def check_for_new_messages(context):
    user_id = context.job.context
    logger.info(f'Проверка новых сообщений для {user_id}')
    ejuser = cte.get_cte(chat_id=user_id)
    new_messages = ejuser.download_messages_preview(limit=10, folder=MessageFolder.INBOX)
    logger.info(f'{len(new_messages)} новых сообщений для {user_id}')
    if not new_messages:
        return
    for message in new_messages:
        text = "<b>Новое сообщение</b>\n\n"
        subject = message['subject']
        files = '📎 ' if message['with_files'] else ''
        unread = '🆕 ' if message['unread'] else ''
        text += f"<b>{unread}{files}<i>{format_user(message['user_from'])}</i></b>" \
                f"<pre>    {subject}</pre>\n"
        keyboard = [[InlineKeyboardButton("Посмотреть сообщение",
                                          callback_data=f'message_view_new_{message["id"]}')]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        context.bot.send_message(chat_id=user_id, text=text, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def messages_common_part(msgs: Dict[str, Union[str, list, int]],
                         folder: str,
                         context: CallbackContext,
                         ejuser: CachedTelegramEljur):
    total = math.ceil(int(msgs['total']) / 6)
    messages_s = present_messages(chat_id=ejuser.chat_id, msgs=msgs, folder=folder)
    op_folder = opposite_folder(folder=folder)
    op_folder_name = folder_to_string(folder=op_folder)
    messages_s = messages_s[:-1]
    messages_s += f"{folder_to_string(folder=folder)} " \
                  f"- страница <b>{context.user_data['messages_page']}/{total}</b>"
    if op_folder == MessageFolder.SENT:
        unread = ejuser.unread_count()
        if unread > 0:
            messages_s += f'\nНовых сообщений: {unread}\n'
    if context.user_data['messages_page'] == 1:
        keyboard = [[InlineKeyboardButton(f'{op_folder_name.lower().capitalize()}',
                                          callback_data=f'page_{op_folder}_1'),
                     InlineKeyboardButton(f'🔄', callback_data=f'page_{folder}_1'),
                     InlineKeyboardButton('➡', callback_data=f'page_{folder}_next')]
                    ]
    else:
        keyboard = [[InlineKeyboardButton('⬅', callback_data=f'page_{folder}_prev'),
                     InlineKeyboardButton('В начало', callback_data=f'page_{folder}_1'),
                     InlineKeyboardButton('➡', callback_data=f'page_{folder}_next')]]
    for i in range(0, msgs['count'], 3):
        keyboard.append([InlineKeyboardButton(str(label),
                                              callback_data=f'message_{folder}_{msgs["messages"][label - 1]["id"]}')
                         for label in range(i + 1, i + 4)])
    reply_markup = InlineKeyboardMarkup(keyboard)
    return messages_s, reply_markup


def messages(update: Update, context: CallbackContext):
    ejuser = cte.get_cte(chat_id=update.message.chat.id)
    msgs = ejuser.get_messages()
    context.user_data['messages_page'] = 1
    folder = MessageFolder.INBOX
    messages_s, reply_markup = messages_common_part(msgs=msgs, folder=folder, context=context, ejuser=ejuser)
    update.message.reply_text(messages_s, parse_mode=ParseMode.HTML, reply_markup=reply_markup)


def messages_page_handler(update: Update, context: CallbackContext):
    try:
        query = update.callback_query
        action = query.data.split('_')[-1]
        if 'messages_page' in context.user_data:
            if action == 'next':
                context.user_data['messages_page'] += 1
            elif action == 'prev':
                context.user_data['messages_page'] -= 1
            elif action.isdigit():
                context.user_data['messages_page'] = int(action)
        else:
            context.user_data['messages_page'] = 1
        folder = query.data.split('_')[1]
        context.user_data['messages_page'] = max(1, context.user_data['messages_page'])
        ejuser = cte.get_cte(chat_id=query.message.chat.id)
        msgs = ejuser.get_messages(page=context.user_data['messages_page'], folder=folder)
        total = math.ceil(int(msgs['total']) / 6)
        if context.user_data['messages_page'] > total:
            context.user_data['messages_page'] = 1
            msgs = ejuser.get_messages(page=context.user_data['messages_page'])
        messages_s, reply_markup = messages_common_part(msgs=msgs, folder=folder, context=context, ejuser=ejuser)
        query.edit_message_text(messages_s, parse_mode=ParseMode.HTML)
        query.edit_message_reply_markup(reply_markup=reply_markup)
        query.answer()
    except:
        print(traceback.format_exc())


def parse_message(message: dict):
    recipients = ''
    for user in message['user_to'][:RECIPIENTS_PREVIEW_COUNT]:
        if user['middlename']:
            recipients += f"{user['lastname']} {user['firstname'][0]}.{user['middlename'][0]}, "
        else:
            recipients += f"{user['lastname']} {user['firstname']}, "
    recipients = recipients[:-2]
    yet_more = len(message['user_to']) - RECIPIENTS_PREVIEW_COUNT
    and_yet_more = f" и ещё {yet_more} {morph.parse('получателей')[0].make_agree_with_number(yet_more).word}" \
        if len(message['user_to']) > RECIPIENTS_PREVIEW_COUNT else ""
    files = ''
    if 'files' in message:
        for file in message['files']:
            files += f'<a href="{file["link"]}">📎 {file["filename"]}</a>\n'
    result = f"<i>Тема:</i> <b>{message['subject']}</b>\n" \
             f"<i>Отправитель:</i> {format_user(message['user_from'])}\n" \
             f"<i>{'Получатели' if len(message['user_to']) > 1 else 'Получатель'}:</i> {recipients}{and_yet_more}\n\n" \
             f"<i>Сообщение:\n</i>" \
             f"<pre>{message['text']}</pre>\n" \
             f"{files}"
    return result


def view_message(update: Update, context: CallbackContext):
    try:
        query = update.callback_query
        ejuser = cte.get_cte(chat_id=query.message.chat.id)
        context.user_data['recipients_offset'] = 0
        context.user_data['reply'] = None
        message_id = query.data.split('_')[-1]
        if query.data.startswith('message_view_new_'):
            keyboard = [[InlineKeyboardButton("Ответить", callback_data=f'reply_inbox_{message_id}'),
                         InlineKeyboardButton("Закрыть", callback_data='close')]]
            message_folder = 'inbox'
        else:
            message_folder = query.data.split('_')[-2]
            keyboard = [[InlineKeyboardButton("Ответить", callback_data=f'reply_{message_folder}_{message_id}'),
                         InlineKeyboardButton("Назад", callback_data=f'page_{message_folder}_it')]]
        message = ejuser.get_message(msg_id=message_id, force_folder=message_folder)
        result = parse_message(message=message)
        ejuser.mark_as_read(msg_id=message_id, folder=message_folder)
        yet_more = len(message['user_to']) - RECIPIENTS_PREVIEW_COUNT
        if yet_more > 0:
            keyboard.append([InlineKeyboardButton("Полный список получателей",
                                                  callback_data=f"recipients_{message_folder}_{message_id}_it")])
        chain = ejuser.messages_chain(msg_id=message_id, folder=message_folder)
        pos_in_chain = 0
        for msg in chain:
            if msg['id'] == message_id:
                break
            pos_in_chain += 1
        if len(chain) > 1:
            if pos_in_chain == 0:
                next_msg = chain[pos_in_chain + 1]
                keyboard.append([InlineKeyboardButton("➡", callback_data=f"message_{next_msg['folder']}_{next_msg['id']}")])
            else:
                if pos_in_chain + 1 < len(chain):
                    next_msg = chain[pos_in_chain + 1]
                    prev_msg = chain[pos_in_chain - 1]
                    keyboard.append([InlineKeyboardButton("⬅",
                                                          callback_data=f"message_{prev_msg['folder']}_{prev_msg['id']}"),
                                     InlineKeyboardButton("➡",
                                                          callback_data=f"message_{next_msg['folder']}_{next_msg['id']}")])
                else:
                    prev_msg = chain[pos_in_chain - 1]
                    keyboard.append(
                        [InlineKeyboardButton("⬅", callback_data=f"message_{prev_msg['folder']}_{prev_msg['id']}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_text(result, parse_mode=ParseMode.HTML)
        query.edit_message_reply_markup(reply_markup)
        query.answer()
    except:
        print(traceback.format_exc())


def close_message(update: Update, context: CallbackContext):
    query = update.callback_query
    context.bot.delete_message(chat_id=query.message.chat.id, message_id=query.message.message_id)
    query.answer()


def message_recipients(update: Update, context: CallbackContext):
    try:
        query = update.callback_query
        ejuser = cte.get_cte(chat_id=query.message.chat.id)
        message_id = query.data.split('_')[-2]
        action = query.data.split('_')[-1]
        folder = query.data.split('_')[1]
        if action == 'next':
            context.user_data['recipients_offset'] += RECIPIENTS_PER_PAGE
        elif action == 'prev':
            context.user_data['recipients_offset'] -= RECIPIENTS_PER_PAGE
        context.user_data['recipients_offset'] = max(0, context.user_data['recipients_offset'])
        if 'messages_folder' not in context.user_data:
            context.user_data['messages_folder'] = MessageFolder.INBOX
        offset = context.user_data['recipients_offset']
        message = ejuser.get_message(message_id, force_folder=context.user_data['messages_folder'])
        total = math.ceil(len(message["user_to"]) / RECIPIENTS_PER_PAGE)
        cur_page = offset // RECIPIENTS_PER_PAGE + 1
        recipients = f'<b>Получатели (страница {cur_page}/{total})</b>\n\n<i>'
        for user in message['user_to'][offset:offset + RECIPIENTS_PER_PAGE]:
            recipients += f"{format_user(user)}, "
        recipients = recipients[:-2]
        recipients += '</i>'
        query.edit_message_text(recipients, parse_mode=ParseMode.HTML)
        if offset > 0:
            keyboard = [[InlineKeyboardButton('⬅', callback_data=f'recipients_{folder}_{message_id}_prev')]]
        else:
            keyboard = []
        if cur_page != total:
            if offset > 0:
                keyboard[0].append(InlineKeyboardButton('➡', callback_data=f'recipients_{folder}_{message_id}_next'))
            else:
                keyboard = [[InlineKeyboardButton('➡', callback_data=f'recipients_{folder}_{message_id}_next')]]
        keyboard.append([InlineKeyboardButton("Назад", callback_data=f'message_{context.user_data["messages_folder"]}'
                                                                     f'_{message_id}')])
        reply_markup = InlineKeyboardMarkup(keyboard)
        query.edit_message_reply_markup(reply_markup)
        query.answer()
    except:
        logger.error(traceback.format_exc())


def message_reply(update: Update, context: CallbackContext):
    query = update.callback_query
    message_id = query.data.split('_')[-1]
    folder = query.data.split('_')[-2]
    ejuser = cte.get_cte(chat_id=query.message.chat.id)
    message = ejuser.get_message(message_id)
    result = parse_message(message=message)
    result += '\n\nНапишите ответное сообщение:'
    query.edit_message_text(result, parse_mode=ParseMode.HTML)
    context.user_data['reply'] = message_id
    keyboard = [[InlineKeyboardButton('Отмена', callback_data=f'message_{folder}_{message_id}')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    query.edit_message_reply_markup(reply_markup)
    query.answer()


def just_message(update: Update, context: CallbackContext):
    if 'reply' in context.user_data and context.user_data['reply']:
        ejuser = cte.get_cte(chat_id=update.message.chat.id)
        message_id = context.user_data['reply']
        context.user_data['reply'] = None
        reply_text = update.message.text
        if ejuser.reply_message(replyto=message_id, text=reply_text):
            message = ejuser.get_message(message_id)
            result = parse_message(message=message)
            result += f'\n\n<b>Ваш ответ на это сообщение был был отправлен:</b> <pre>{reply_text}</pre>'
            update.message.reply_text(result, parse_mode=ParseMode.HTML)
        else:
            update.message.reply_text('Произошла ошибка, попробуйте позднее')
    else:
        send_menu(update=update, context=context)


# noinspection PyBroadException
def cache_full_messages_task():
    while True:
        for chat_id in authorized_chat_ids:
            time_begin = time.time()
            try:
                ejuser = cte.get_cte(chat_id=chat_id)
                ejuser.cache_full_messages()
            except:
                logger.error(f'Ошибка кэширования сообщений для {chat_id}, traceback:\n{traceback.format_exc()}')
            logger.info(f'Работа по кэшированию сообщений для {chat_id} завершена '
                        f'за {(int(time.time() - time_begin) * 1000)} ms')
        time.sleep(MESSAGES_CACHE_DELAY)


if __name__ == '__main__':
    persistence = PicklePersistence(filename=str(data_dir / 'persistence.pickle'))
    updater = Updater(os.environ["token"], use_context=True, persistence=persistence)

    callback_queries = [
        {'callback': login_handler, 'pattern': '^login$'},
        {'callback': homework_handler, 'pattern': '^homework_[0-9.]*$'},
        {'callback': messages_page_handler, 'pattern': '^(page_inbox_|page_sent_)(prev|next|it|[0-9]*)*$'},
        {'callback': view_message, 'pattern': '^(message_inbox_|message_sent_|message_view_new_)[0-9]*$'},
        {'callback': message_reply, 'pattern': '^(reply_inbox_|reply_sent_)[0-9]*$'},
        {'callback': message_recipients, 'pattern': '^(recipients_inbox_|recipients_sent_)[0-9]*_(prev|next|it)$'},
        {'callback': close_message, 'pattern': '^close$'}
    ]
    for param in callback_queries:
        updater.dispatcher.add_handler(CallbackQueryHandler(**param))
    updater.dispatcher.add_error_handler(error)

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler('start', start),
                      MessageHandler(Filters.regex('Попробовать ещё раз'), login_handler)],
        states={
            LOGIN: [MessageHandler(Filters.regex('Войти в элжур'), login_handler)],
            WAIT_LOGIN: [MessageHandler(Filters.text, user_send_login)],
            WAIT_PASSWORD: [MessageHandler(Filters.text, user_send_password)],
            MAIN_MENU: [MessageHandler(Filters.regex('Домашнее задание'), homework),
                        MessageHandler(Filters.regex('Сообщения'), messages),
                        MessageHandler(Filters.text, just_message)],
        },
        fallbacks=[CommandHandler('cancel', cancel)],
        name="bot_conversation",
        persistent=True,
        per_message=False
    )

    updater.dispatcher.add_handler(conv_handler)
    authorized_chat_ids = [user['chat_id'] for user in data.find({})]

    for uid in authorized_chat_ids:
        updater.job_queue.run_repeating(check_for_new_messages,
                                        interval=MESSAGES_CHECK_DELAY,
                                        first=MESSAGES_CHECK_DELAY,
                                        context=uid)

    Thread(target=cache_full_messages_task, daemon=True, name='Cache-Full').start()

    # Запуск бота
    updater.start_polling()

    # Работать пока пользователь не нажмет Ctrl-C или процесс получит SIGINT,
    # SIGTERM or SIGABRT
    updater.idle()
