#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Any
from html import escape
from datetime import datetime

def fmt_date_ddmmyyyy(s: str) -> str:
    if not s:
        return ""
    try:
        return datetime.strptime(s, "%Y-%m-%d").strftime("%d.%m.%Y")
    except ValueError:
        return s

def fmt_dt_ddmmyyyy(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    try:
        # 2025-09-26T08:30:00.000Z
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M")
        # fallback старый формат
        return datetime.strptime(s, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    except Exception:
        return s

def read_csv(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def num(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()

def esc(v: Any) -> str:
    s = "" if v is None else str(v)
    return escape(s)

def kcal_badge(fact: str, target: str) -> str:
    try:
        f = float(fact)
        t = float(target)
        if t <= 0:
            return "badge"
        ratio = f / t
        if ratio <= 1.05:
            return "badge badge-ok"
        if ratio <= 1.15:
            return "badge badge-warn"
        return "badge badge-warn"
    except Exception:
        return "badge"

def render_day_block(day: dict, meals: List[dict]) -> str:
    dk = esc(day.get("День питания", ""))

    raw_day_date = (day.get("Дата", "") or "").strip()
    day_date = fmt_date_ddmmyyyy(raw_day_date)

    fact_k = num(day.get("Калории приема пищи", ""))
    t_k = num(day.get("Цель по калориям", ""))

    badge_cls = kcal_badge(fact_k, t_k)

    def fmt_g(x):
        return (num(x) + " г") if num(x) != "" else ""

    html = []
    html.append('<section class="day-card">')

    html.append('<header class="day-head">')
    html.append('<div class="day-title">')
    html.append(f"<h2>{dk}</h2>")
    html.append(f'<div class="day-sub">Дата: {esc(day_date)}</div>')
    html.append("</div>")
    html.append('<div class="badges">')
    if t_k != "":
        html.append(f'<span class="badge">Цель: {esc(t_k)} ккал</span>')
    if fact_k != "":
        html.append(f'<span class="{badge_cls}">Факт: {esc(fact_k)} ккал</span>')
    html.append("</div>")
    html.append("</header>")

    html.append('<div class="day-grid">')

    html.append('<div class="panel"><h3>Вес</h3><div class="kv">')
    html.append(f'<div class="k">Утро</div><div class="v">{esc(day.get("Вес утром",""))}</div>')
    html.append(f'<div class="k">Вечер</div><div class="v">{esc(day.get("Вес вечером",""))}</div>')
    html.append(f'<div class="k">Средний</div><div class="v">{esc(day.get("Средний вес за день",""))}</div>')
    html.append('</div></div>')

    html.append('<div class="panel"><h3>Итоги питания</h3><div class="kv">')
    html.append(f'<div class="k">Ккал</div><div class="v">{esc(day.get("Калории приема пищи",""))}</div>')
    html.append(f'<div class="k">Б</div><div class="v">{esc(fmt_g(day.get("Белки приема пищи","")))}</div>')
    html.append(f'<div class="k">Ж</div><div class="v">{esc(fmt_g(day.get("Жиры приема пищи","")))}</div>')
    html.append(f'<div class="k">У</div><div class="v">{esc(fmt_g(day.get("Углеводы приема пищи","")))}</div>')
    html.append('</div></div>')

    html.append('<div class="panel"><h3>Цели</h3><div class="kv">')
    html.append(f'<div class="k">Ккал</div><div class="v">{esc(day.get("Цель по калориям",""))}</div>')
    html.append(f'<div class="k">Б</div><div class="v">{esc(fmt_g(day.get("Цель по белкам","")))}</div>')
    html.append(f'<div class="k">Ж</div><div class="v">{esc(fmt_g(day.get("Цель по жирам","")))}</div>')
    html.append(f'<div class="k">У</div><div class="v">{esc(fmt_g(day.get("Цель по углеводам","")))}</div>')
    html.append('</div></div>')

    html.append("</div>")  # day-grid

    note = day.get("Описание", "") or ""
    html.append('<div class="note"><h3>Описание</h3>')
    html.append(f'<div class="note-body">{esc(note) if note else ""}</div></div>')

    html.append('<div class="meals"><h3>Приёмы пищи</h3>')
    if not meals:
        html.append('<div class="meal"><div class="meal-head"><div class="meal-title">Нет записей</div></div></div>')
    else:
        for m in meals:
            html.append('<div class="meal">')
            html.append('<div class="meal-head">')
            html.append(f'<div class="meal-title">{esc(m.get("Тип приема пищи",""))}</div>')

            raw_meal_dt = (m.get("Дата и время", "") or "").strip()
            meal_dt = fmt_dt_ddmmyyyy(raw_meal_dt)

            meta_parts = []
            if meal_dt:
                meta_parts.append(meal_dt)
            kind = (m.get("Вид приема пищи", "") or "").strip()
            if kind:
                meta_parts.append(kind)
            meta = " · ".join(meta_parts)

            html.append(f'<div class="meal-meta">{esc(meta)}</div>')
            html.append('</div>')  # meal-head

            html.append('<div class="meal-grid">')
            html.append(f'<div class="pill">Ккал: {esc(m.get("Калории приема пищи",""))}</div>')
            html.append(f'<div class="pill">Б: {esc((num(m.get("Белки приема пищи","")) + " г") if num(m.get("Белки приема пищи","")) else "")}</div>')
            html.append(f'<div class="pill">Ж: {esc((num(m.get("Жиры приема пищи","")) + " г") if num(m.get("Жиры приема пищи","")) else "")}</div>')
            html.append(f'<div class="pill">У: {esc((num(m.get("Углеводы приема пищи","")) + " г") if num(m.get("Углеводы приема пищи","")) else "")}</div>')
            html.append('</div>')  # meal-grid

            mnote = m.get("Описание", "") or ""
            if mnote:
                html.append(f'<div class="meal-note">{esc(mnote)}</div>')
            html.append('</div>')  # meal
    html.append("</div>")  # meals

    html.append("</section>")
    return "\n".join(html)

def chunk(lst: List[Any], size: int) -> List[List[Any]]:
    return [lst[i:i + size] for i in range(0, len(lst), size)]

def day_id_from_day(day: dict) -> str:
    # DailyLog: "Дата" обычно YYYY-MM-DD
    return (day.get("DayDate") or day.get("Дата") or "").strip()[:10]

def day_id_from_meal(m: dict) -> str:
    # Meals: есть DayDate; если нет — возьмём первые 10 символов из ISO datetime
    return (m.get("DayDate") or (m.get("Дата и время") or "")[:10] or "").strip()[:10]

def build_html(daily_csv: Path, meals_csv: Path, out_html: Path):
    days = read_csv(daily_csv)
    meals = read_csv(meals_csv)

    by_day: Dict[str, List[dict]] = defaultdict(list)
    for m in meals:
        did = day_id_from_meal(m)
        if did:
            by_day[did].append(m)

    for did, arr in by_day.items():
        arr.sort(key=lambda x: (x.get("Дата и время") or ""))

    days.sort(key=lambda d: (d.get("Дата") or "", d.get("День питания") or ""))

    pages = chunk(days, 2)

    doc = []
    doc.append("<!doctype html>")
    doc.append("<meta charset='utf-8'>")
    doc.append("<meta name='viewport' content='width=device-width, initial-scale=1'>")

    # ВАЖНО: report лежит в data_dumps/dump_<tag>/, стили в data_dumps/styles.css
    doc.append('<link rel="stylesheet" href="../styles.css">')

    doc.append("<title>Export report</title>")
    doc.append("<body><div class='container'>")
    doc.append("<div class='header'>")
    doc.append("<h1>Экспорт: Дневник питания и Приёмы пищи</h1>")
    doc.append(f"<div class='meta'>Generated: {escape(datetime.utcnow().strftime('%d.%m.%Y'))}</div>")
    doc.append("</div>")

    for page in pages:
        doc.append("<div class='page'>")
        for day in page:
            did = day_id_from_day(day)
            doc.append(render_day_block(day, by_day.get(did, [])))
        doc.append("</div>")

    doc.append("</div></body>")
    out_html.parent.mkdir(parents=True, exist_ok=True)
    out_html.write_text("\n".join(doc), encoding="utf-8")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--dailylog", required=True)
    p.add_argument("--meals", required=True)
    p.add_argument("--out", required=True)
    args = p.parse_args()

    build_html(Path(args.dailylog), Path(args.meals), Path(args.out))
    print(f"Wrote: {args.out}")
