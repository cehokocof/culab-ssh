# Задача-демо для проверки culab end-to-end

Цель: за один заход прогнать весь набор `./culab`-команд на коротком проекте.
Должно укладываться в ~15 секунд.

## Что сделать

Подготовь локально каталог `/tmp/culab_demo` из трёх файлов:

- `pyproject.toml` — минимальный (`name = "culab-demo"`, `requires-python = ">=3.10"`, `dependencies = ["requests"]`).
- `main.py` — `import requests` + `print(f"requests=={requests.__version__}")`.
- `worker.py` — цикл из 5 итераций, в каждой `print("step N/5", flush=True)` и `time.sleep(0.6)`, в конце `print("worker done")`.

Через `./culab` выполни конвейер (`REMOTE=/home/jovyan/culab_demo`):

1. `./culab ping` — убедись что демон живой и запомни PID.
2. `./culab terminals` — сохрани `total` и `ours`, чтобы в конце сравнить.
3. `./culab push /tmp/culab_demo $REMOTE` — должна вернуться **одна** строка `pushed 3 files (...)`.
4. `./culab exec "rm -rf $REMOTE/.venv && cd $REMOTE && python3 -m venv .venv && .venv/bin/pip install --quiet --disable-pip-version-check requests" --timeout 120` — создать venv и поставить туда requests.
5. `./culab exec "cd $REMOTE && .venv/bin/python main.py"` — должно напечатать `requests==X.Y.Z` (используем созданный venv).
6. `JID=$(./culab spawn "cd $REMOTE && .venv/bin/python worker.py")` — должен напечататься 8-символьный hex.
7. `./culab status $JID` сразу — ожидается `{"running":true,"pid":N}`.
8. `./culab jobs` — `JID` должен присутствовать.
9. `sleep 1.5 && ./culab log $JID --tail 2` — увидишь последние 1–2 строки worker'а.
10. `sleep 2.5 && ./culab log $JID --grep "done"` — должна вернуться строка `worker done`.
11. `./culab status $JID` — теперь `{"running":false,"rc":0,...}`.
12. `./culab reap $JID` — `{"ok":true}`.
13. `./culab terminals` — сравни с шагом 2: `total` и `ours` **не должны вырасти**.

## Контракт прохождения

- В отчёте отдельно укажи **PID демона** (из ping) и **rc worker'а** (из status #11) — это два самых чувствительных индикатора, что всё прошло на одном демоне.
- Если на каком-то шаге был ретрай и `total` терминалов вырос — это норма (DNS-блип, captcha), но отметь это явно.
- `status` после `reap` дал бы `{"error":"unknown_job"}` — это ожидаемо, проверять не нужно.

## Опциональные шаги

Если хочешь покрыть и `kill`/`cleanup`:

- В шаге 6 запусти что-нибудь долгое (`sleep 60`), на шаге 9 сделай `./culab kill $JID`, затем `status` (`running:false`, `rc=-15` или похожее).
- В конце `./culab cleanup --ours` чтобы убрать наш терминал и начать следующую сессию с нуля.

## Что НЕ нужно делать

- Не используй `./culab reset` без причины — он удалит наш терминал и потеряет состояние всех текущих job.
- Не запрашивай `./culab log` без `--tail` или `--grep` — на больших job это съест контекст.
- Не вызывай команды burst'ом (5+ подряд без пауз) — Yandex anti-bot может поймать.
