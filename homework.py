import traceback
from textwrap import wrap

from pymorphy2 import MorphAnalyzer
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import CallbackContext

from CTEStorage import cte

morph = MorphAnalyzer()


def get_homework(date: str, hw: dict):
    lessons = list(hw[date]['items'].keys())
    lessons_s = ''
    ind = 1
    files = ''
    for lesson in lessons:
        c_hw = ''
        for item in hw[date]['items'][lesson]['homework'].values():
            wrapped = wrap(item['value'], 50)
            wrapped_s = ('\n' + " " * 4).join(wrapped)
            c_hw += " " * 2 + f"👉 {wrapped_s}\n"
        if 'files' in hw[date]['items'][lesson]:
            for file in hw[date]['items'][lesson]['files']:
                files += f'<a href="{file["link"]}">📎 {file["filename"]}</a>\n'
        lessons_s += f'{ind}. <b>{lesson}</b>: \n{c_hw}'
        ind += 1

    day_of_week = morph.parse(hw[date]['title'])[0].inflect({'datv'}).word

    tasks = f"Задание к {day_of_week} {date}:\n<pre>{lessons_s}</pre>\n" + files
    return tasks


def homework(update: Update, context: CallbackContext):
    try:
        ejuser = cte.get_cte(chat_id=update.message.chat.id)
        hw = ejuser.homework
        dates = list(hw.keys())
        date_buttons = [InlineKeyboardButton(f'{".".join(label.split(".")[:-1])} ({hw[label]["title"].lower()})',
                                             callback_data=f'homework_{label}') for label in dates]
        date_buttons_split = []
        for i in range(0, len(date_buttons), 2):
            date_buttons_split.append(date_buttons[i:i + 2])
        keyboard = [*date_buttons_split]
        reply_markup = InlineKeyboardMarkup(keyboard)
        update.message.reply_text('📝 Домашнее задание на:', reply_markup=reply_markup)
    except:
        print(traceback.format_exc())


def homework_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()

    date = query.data.split('_')[-1]

    ejuser = cte.get_cte(chat_id=query.message.chat.id)
    hw = ejuser.homework

    dates = list(hw.keys())
    date_buttons = [InlineKeyboardButton(f'{".".join(label.split(".")[:-1])} ({hw[label]["title"].lower()})',
                                         callback_data=f'homework_{label}') for label in dates]
    date_buttons_split = []
    for i in range(0, len(date_buttons), 2):
        date_buttons_split.append(date_buttons[i:i + 2])
    keyboard = [*date_buttons_split]
    reply_markup = InlineKeyboardMarkup(keyboard)
    tasks = get_homework(date=date, hw=hw)

    query.edit_message_text(text=tasks, parse_mode='html')
    query.edit_message_reply_markup(reply_markup=reply_markup)
