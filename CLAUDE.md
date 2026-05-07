# Проект: Платформа «Мои практики»

Telegram-бот + Mini App для трекинга регулярных практик с админкой, рейтингом участников и настраиваемыми напоминаниями.

## Стек

- **Backend:** FastAPI + aiogram 3 + APScheduler + SQLite — всё в одном файле `server.py`
- **Frontend:** React через Babel-standalone (без сборки!) + Tailwind через CDN
- **Auth:** Telegram WebApp `initData` с проверкой HMAC-подписи токеном бота
- **Деплой:** GitHub Actions → SSH → VPS с systemd + Caddy (HTTPS)

## Структура

```
server.py                      — единый бэкенд: API, бот, планировщик, БД
frontend/app.html              — Mini App для участников (/app)
frontend/admin.html            — админка (/admin), доступ по ADMIN_IDS
requirements.txt
.env                           — секреты (НЕ коммитить!)
.env.example                   — шаблон для разработчиков
deploy/setup-vps.sh            — первоначальная настройка сервера
.github/workflows/deploy.yml   — автодеплой при push в main
```

## Принципы разработки

- **Один файл `server.py`** — не дроби на модули без серьёзной причины
- **Без Node-сборки** — фронт через Babel-standalone, чтоб любой мог открыть и поправить
- **Стиль:** тёплая палитра (амбра `#d4a857`), шрифты Cormorant Garamond + Manrope
- **Язык интерфейса:** русский
- **Секреты:** только в `.env`, никогда в коде, никогда в git

## Перед коммитом

- Проверить что Python запускается: `python3 -m py_compile server.py`
- Запустить сервер на 5 секунд и убедиться что нет ошибок старта:
  `BOT_TOKEN= ADMIN_IDS= timeout 5 python3 server.py`
- Открыть `/app` и `/admin` локально, убедиться что фронт грузится

## Как обновлять прод

После `git push origin main` GitHub Actions сам:
1. Зайдёт на VPS по SSH
2. Сделает `git pull`
3. Поставит зависимости из `requirements.txt`
4. Перезапустит `systemctl restart practices`

Деплой ~30 секунд. Если что-то сломалось — `ssh user@host journalctl -u practices -n 50`.

## База данных

SQLite-файл `/opt/practices/data.db` на VPS. **В git не входит** (см. `.gitignore`).
Бэкап: просто `scp` файла на свою машину, или cron-задача на VPS.

При изменении схемы (`SCHEMA` в `server.py`) — добавляй `ALTER TABLE` вручную, **не** теряй существующие данные.

## Безопасность

- `BOT_TOKEN` — секрет, токен бота. С его помощью Telegram подписывает initData, без него никто не аутентифицируется.
- `ADMIN_IDS` — список Telegram ID через запятую. Только эти юзеры видят `/admin`.
- Все API эндпоинты требуют валидный заголовок `X-Init-Data`.

## Что МОЖНО менять без опасений

- Текст, цвета, шрифты, размеры в HTML-файлах
- Добавлять новые эндпоинты в `server.py`
- Менять логику отображения, формы, кнопки
- Добавлять команды бота (`@dp.message(Command(...))`)

## Что требует ОСТОРОЖНОСТИ

- Изменения в `SCHEMA` — могут потребовать миграции данных
- Изменения в `verify_init_data` — могут сломать аутентификацию
- Удаление таблиц или полей — потеря данных юзеров

## Полезные команды для отладки на VPS

```bash
# Логи приложения в реальном времени
sudo journalctl -u practices -f

# Перезапустить руками
sudo systemctl restart practices

# Проверить что работает
sudo systemctl status practices

# Посмотреть БД
sudo -u practices sqlite3 /opt/practices/data.db ".tables"
```

## Расширение функциональности

При добавлении больших фич:
1. Сначала **plan mode** (`shift+tab` в Claude Code) — обсудить план без правок файлов
2. После одобрения — применить изменения
3. Прогнать локальную проверку (см. «Перед коммитом»)
4. Коммит с осмысленным сообщением (`feat: ...`, `fix: ...`)
5. Push → дождаться зелёного значка в GitHub Actions
