# culab — заметка для AI-агентов

Этот репозиторий — мост к JupyterHub `jupyter.culab.ru`. У тебя нет SSH и нет sudo на сервере; ты ходишь туда через WebSocket терминал, авторизуясь экспортированными браузерными cookies (`cookies.json`).

**Единственная точка входа — обёртка `./culab`.** Не запускай `python3 jupyter_terminal_exec.py`, `./remote-run` напрямую, `python3 remote_push_code.py` или `jupyter_chunk_upload.py` — это устаревший путь. Все `push-*` скрипты уже переписаны на `./culab push` и тоже пригодны.

## Когда использовать

- Запустить команду на сервере → `./culab exec`.
- Запустить долгую задачу (обучение, скачивание, билд) → `./culab spawn` + опрос.
- Залить локальный проект (фильтр в `remote_push_code.py`) → `./culab push` или готовые `./push-*`.
- Прочитать файл с сервера → `./culab exec "cat /path/to/file"`.
- Записать файл на сервер маленький → `./culab exec` + heredoc, большой → `./culab push` целого каталога.

## Команды и контракт вывода

| команда | что печатает в stdout | exit code |
|---|---|---|
| `./culab exec "<cmd>" [--cwd PATH] [--timeout SEC]` | **чистый stdout** удалённой команды; stderr на stderr | **rc удалённой команды** (не 0, если упало) |
| `./culab spawn "<cmd>" [--cwd PATH]` | только `job_id` (8-символьный hex) | 0 если стартовал |
| `./culab status JID` | `{"running":true,"pid":N}` или `{"running":false,"rc":0,"pid":N}` | 0 |
| `./culab log JID [--tail N] [--grep PATTERN]` | сырой stdout+stderr процесса | 0 |
| `./culab kill JID [--signal SIGTERM]` | `{"ok":true,"signal":"..."}` | 0 |
| `./culab jobs` | `{"jobs":[...]}` — что запомнил демон | 0 |
| `./culab reap JID` | `{"ok":true}` | 0 |
| `./culab push LOCAL_DIR REMOTE_DIR [--exclude PATH]...` | одна строка `pushed N files (B bytes, C chunks) -> REMOTE_DIR` | 0 если sha256 совпал |
| `./culab ping` | `{"pong":true,"ts":...,"pid":...}` (PID серверного демона) | 0 |
| `./culab terminals` | JSON со всеми терминалами в JupyterHub, наш помечен `"ours":true` | 0 |
| `./culab cleanup --ours \| --name X` | `{"ok":true,"removed":[...]}` | 0 |
| `./culab reset` | `{"ok":true,"removed_terminal":"..."}` | 0 |

Обещание: stdout не содержит ANSI, prompt'ов, эха, маркеров — только то, что напечатала команда.

## Правила времени

- **Команда заведомо < ~20 секунд** → `exec`. Дефолтный `--timeout=120`.
- **Команда может длиться дольше** или вообще unbounded → `spawn`. Иначе ты подвесишь WebSocket и/или поймаешь таймаут.
- При работе со `spawn`:
  - Большой лог читай с `--tail N` или `--grep`, **не целиком**. Лог хранится на сервере в `~/.cache/culab-jobs/<jid>.log` и переживает рестарт демона.
  - `status` — это in-memory таблица демона; **после рестарта демона** (рассинхронизация WS, pod redeploy) `status` для старых job вернёт `{"error":"unknown_job"}`. Лог при этом цел — читай его.
  - Когда задача больше не нужна — `reap` чтобы убрать лог-файл с диска.

## Push: что фильтруется

`./culab push` берёт фильтр из `remote_push_code.py` (`should_include`):
- Включает: `*.py *.ipynb *.toml *.lock *.md *.txt *.yaml *.yml *.json *.ini *.cfg *.sh`, `.gitignore .dockerignore .python-version`, `Dockerfile Makefile`, **а также `Dockerfile.* / *.Dockerfile / Makefile.*`** (например `Dockerfile.vllm`).
- Исключает: `.git .venv venv __pycache__ node_modules outputs artifacts checkpoints models .pytest_cache .mypy_cache .ruff_cache .DS_Store`, `*.csv *.parquet *.pkl *.pickle *.joblib *.db *.zip *.tgz *.tar *.gz *.pt *.pth *.ckpt *.safetensors *.onnx`, и `data/*.json` `data/*.npz`.
- Точечно исключить файл — `--exclude path/relative/to/project`.

После распаковки на сервере проверяется `sha256sum -c .codex_push_manifest.sha256` — если хоть один файл побит, `./culab push` упадёт с rc≠0.

## Чего НЕ делать

1. **Не делать burst-вызовы** (`./culab status JID` в цикле без sleep). JupyterHub-фронт прячет за Yandex anti-bot — поймаешь `tmgrdfrend/showcaptcha` и придётся переэкспортировать cookies. Если опрашиваешь длинный job — `sleep 5` или больше между опросами. Throttle на 0.6с уже встроен, но это нижняя граница.
2. **Никогда `./culab cleanup --all`** — этой опции нет специально. Чужие терминалы (`ours: false`) могут быть твоими собственными активными сессиями. Удаляй только по точному имени или `--ours`.
3. **Не запускай старые скрипты прямого WS-вывода** (`./remote-run` через `python3 jupyter_terminal_exec.py`) — они засоряют твой контекст PTY-мусором.
4. **Не читай большие логи целиком**. Всегда `--tail` или `--grep`.

## Восстановление после ошибок

- `RuntimeError: websocket closed` / `EOFError` — встроенный retry уже один раз отработал. Если упал снова — `./culab reset` и повтори. Возможно pod jupyterhub'a рестартанул.
- `tmgrdfrend/showcaptcha` — поймал captcha. Скажи пользователю переэкспортировать `cookies.json` из браузера.
- `{"error":"sha256_mismatch", ...}` в push — не должно происходить (chunks идемпотентны через `offset`). Если случилось — `./culab reset` и повтори push.
- `unknown_job` от `status`/`log`/`kill` — демон рестартанул и забыл in-memory таблицу. Лог на диске всё ещё есть: `./culab exec "ls ~/.cache/culab-jobs/"` и `./culab exec "tail -50 ~/.cache/culab-jobs/<jid>.log"`.

## Где живёт состояние

- Локально: `~/.cache/culab/rpc.json` — имя нашего сохранённого терминала + timestamp последнего connect (для throttle).
- На сервере:
  - демон-процесс живёт в JupyterHub-терминале, имя в state-файле выше.
  - `~/.cache/culab-jobs/<jid>.log` — логи фоновых задач.
  - `~/.cache/culab-jobs/<jid>.cmd` — команда, которой стартовали.

## Переменные окружения

| переменная | по умолчанию | для чего |
|---|---|---|
| `HUB_URL` | `https://jupyter.culab.ru` | endpoint JupyterHub |
| `COOKIES_JSON` | `cookies.json` | путь к экспортированным cookies |
| `CULAB_MIN_INTERVAL` | `0.6` | минимум секунд между WS handshakes |
| `CULAB_CHUNK` | `262144` (256 KB) | размер одного upload-чанка |
| `CULAB_CHUNK_TIMEOUT` | `10` | таймаут на один чанк (сек) |
| `CULAB_MAX_ATTEMPTS` | `3` | попыток на один RPC при сбое WS |
| `CULAB_PTY_PIECE` | `65536` | разбиение больших WS-фреймов |
| `CULAB_PROGRESS` | (нет) | если задан — печатает прогресс push в stderr |

## Полезные шаблоны

**Запустить и проследить тренировку:**
```bash
JID=$(./culab spawn 'cd /home/jovyan/datadojo1 && python3 train.py --epochs 10')
echo "$JID" > /tmp/current-job
# Через какое-то время:
./culab log $(cat /tmp/current-job) --tail 50
./culab status $(cat /tmp/current-job)
# Если надо остановить:
./culab kill $(cat /tmp/current-job)
```

**Залить локальный проект и сразу что-то проверить:**
```bash
./culab push /Users/me/Programming/myproject /home/jovyan/myproject
./culab exec "cd /home/jovyan/myproject && python3 -c 'import myproject; print(myproject.__version__)'"
```

**Удалить один зомби-терминал:**
```bash
./culab terminals    # посмотреть имена
./culab cleanup --name 7
```

## Что под капотом (если очень нужно)

- `culab` — bash wrapper над `culab_rpc.py`.
- `culab_rpc.py` — клиент. Держит `RpcSession` (один WebSocket на серию вызовов), кэширует имя терминала в `~/.cache/culab/rpc.json`, делает throttle, retry, авто-cleanup мёртвых терминалов.
- `culab_rpc_server.py` — серверный демон. Загружается inline через `exec python3 -c "exec(b64decode(...))"`; никаких файлов на сервере не создаётся для bootstrap.
- `jupyter_terminal_exec.py` — низкоуровневый WS-handshake, cookie-auth, framing. Не дёргай напрямую.
- `remote_push_code.py` — фильтр содержимого тарбола (`should_include`). Используется как библиотека внутри `./culab push`.

Старые `./remote-run`, `./push-*` теперь просто тонкие шапки над `./culab` — оставлены для привычки.
