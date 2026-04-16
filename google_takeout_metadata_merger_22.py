"""
Google Takeout Metadata Merger (2026-edition)
==============================================
Merged EXIF-Metadaten aus Google-Takeout-JSON-Dateien in Bilder/Videos.

Unterstützte JSON-Namensformate (alle Varianten die Google produziert):
  - IMG_1234.jpg.json                          (altes Format, vor 2024)
  - IMG_1234.jpg.supplemental-metadata.json    (neues Format, ab 2024)
  - PXL_20230815.jpg.supplemental-metada.json  (truncated, 46-Zeichen-Limit)
  - PXL_20230815.jpg.supplemental-metad.json   (truncated)
  - PXL_20230815.jpg.supplemental-meta.json    (truncated)
  - PXL_20230815.jpg.supplemental-met.json     (truncated)
  - PXL_20230815.jpg.supplemental-me.json      (truncated)
  - PXL_20230815.jpg.supplemental-m.json       (truncated)
  - PXL_20230815.jpg.supplemental-.json        (truncated)
  - PXL_20230815.jpg.supplemental.json         (truncated)
  - PXL_20230815.jpg.supplemen.json            (truncated)
  - ... bis .s.json                             (extrem truncated)
  - IMG_1234(1).jpg.supplemental-metadata.json (Duplikat-Nummerierung)
  - IMG_1234-edited.jpg.json                   (bearbeitete Fotos)

Google Pixel Motion Photos (Sonderfall):
  Google Takeout exportiert Motion Photos als zwei Dateien:
  - PXL_20220622_114604997.MP.jpg  (JPEG-Foto, evtl. mit eingebettetem Video)
  - PXL_20220622_114604997.MP      (Video-Komponente separat, ist ein MP4)
  Beide Dateien teilen sich EINE JSON-Datei (.MP.jpg.json oder .MP.jpg.supplemental…json).
  Dieses Script erkennt diesen Sonderfall und wendet die JSON-Metadaten auf BEIDE an.

Voraussetzung: ExifTool (wird automatisch heruntergeladen falls nicht vorhanden)
"""

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import os
import json
import subprocess
import urllib.request
import zipfile
import shutil
import re
import time
from pathlib import Path
from typing import Optional

# ─── Internationalisation ──────────────────────────────────────────────────────

_STRINGS = {
    "en": {
        # Window / title
        "window_title":      "Google Takeout Metadata Merger  •  2026",
        "app_title":         "Google Takeout  ⟶  EXIF Merger",
        "app_version":       "v2026",
        # Folder row
        "folder_label":      "Takeout folder:",
        "browse_btn":        "  Browse  ",
        # Options
        "dry_run_label":     "Dry run — no files will be modified",
        "workers_label_txt": "Parallel workers:",
        "workers_hint":      "HDD: 1-2 recommended  |  SSD: up to {n}",
        # Buttons
        "start_btn":         "  ▶  Start  ",
        "running_btn":       "  ⏳  Running…  ",
        "clear_log_btn":     "  Clear log  ",
        "delete_json_btn":   "  🗑  Delete JSONs  ",
        "backup_warning":    "⚠ Backup recommended! Files will be overwritten.",
        # Progress
        "ready_label":       "Ready.",
        "done_label":        "Done.",
        "progress_label":    "{cur} / {tot}  ({pct:.0f}%)",
        "log_label":         "Log:",
        # Dialogs
        "dlg_no_folder":     "Please select a valid folder first.",
        "dlg_backup_title":  "Confirm",
        "dlg_backup_msg":    "⚠  Files will be overwritten!\n\nHave you created a backup?\n\nContinue anyway?",
        "dlg_running_title": "Running",
        "dlg_running_msg":   "Please wait until the current run is finished.",
        "dlg_no_dir_title":  "Error",
        "dlg_no_dir_msg":    "Please select a folder first.",
        "dlg_no_json_title": "No JSONs found",
        "dlg_no_json_msg":   "No matching JSON sidecar files found.",
        "dlg_del_title":     "Delete JSONs",
        "dlg_del_msg":       "{n} JSON sidecar files found.\n\nThese belong to media in the selected folder.\n\n⚠  This cannot be undone!\n\nDelete now?",
        "dlg_del_done":      "Done",
        "dlg_del_result":    "{n} JSON files deleted.",
        "dlg_del_errors":    "\n{n} error(s) — see log.",
        "browse_title":      "Select Takeout folder",
        # Log messages (process)
        "log_exiftool_dl":   "ExifTool not found — downloading…",
        "log_exiftool_fail": "\n✗  Aborted: ExifTool could not be installed.",
        "log_exiftool_ok":   "✓  ExifTool: {path}",
        "log_workers":       "⚡  Parallel workers: {n} (of {total} CPU cores)",
        "log_scanning":      "📁  Scanning: {root}\n",
        "log_scan_json":     "Scanning JSON files…",
        "log_scan_json_ok":  "   {n} JSON files found.\n",
        "log_scan_media":    "   {n} media files found.\n",
        "log_preparing":     "Preparing jobs…",
        "log_jobs_ready":    "   {n} jobs prepared.\n",
        "log_dry_run_file":  "  [DRY RUN] Would process: {rel}",
        "log_dry_run_mp":    "  [DRY RUN] Would process (Motion Photo video): {rel}",
        "log_dry_no_json":   "  ⚠  No JSON: {rel}",
        "log_dry_summary":   "\n[DRY RUN] {n} would be processed, {m} without JSON.",
        "log_ok":            "  ✓  {rel}",
        "log_renamed":       "  🔄  {rel}  (was: {old})",
        "log_err":           "  ✗  {rel}\n     {msg}",
        "log_skip":          "  ⚠  No JSON: {rel}",
        "log_json_unread":   "  ✗  JSON unreadable: {name}",
        "log_worker_err":    "  ✗  Worker error: {msg}",
        "log_no_json_hdr":   "\n  ── Files without JSON ({n}) ──",
        "log_summary_sep":   "═" * 60,
        "log_processed":     "  ✓  Processed:       {n}",
        "log_renamed_sum":   "  🔄  Renamed:         {n}",
        "log_no_json_sum":   "  ⚠  No JSON:         {n}",
        "log_errors_sum":    "  ✗  Errors:          {n}",
        "log_total_sum":     "  ━  Total:           {n}",
        "log_done":          "\n🎉  Done!",
        "log_del_ok":        "\n🗑  JSON cleanup: {n} deleted, {e} errors.",
        "log_del_err":       "  ✗  Delete error: {path}\n     {msg}",
        # ExifTool download
        "dl_downloading":    "⬇  Downloading ExifTool…",
        "dl_done":           "✓  Download complete, extracting…",
        "dl_installed":      "✓  ExifTool installed: {path}",
        "dl_exe_missing":    "✗  ExifTool exe not found after extraction!",
        "dl_failed":         "✗  Download failed: {e}",
        "dl_manual":         "   Please download ExifTool manually from https://exiftool.org",
        "dl_manual2":        "   and place exiftool.exe next to this script.",
    },
    "de": {
        "window_title":      "Google Takeout Metadata Merger  •  2026",
        "app_title":         "Google Takeout  ⟶  EXIF Merger",
        "app_version":       "v2026",
        "folder_label":      "Takeout-Ordner:",
        "browse_btn":        "  Durchsuchen  ",
        "dry_run_label":     "Testlauf (Dry-Run) — keine Dateien ändern",
        "workers_label_txt": "Parallele Worker:",
        "workers_hint":      "HDD: 1-2 empfohlen  |  SSD: bis {n}",
        "start_btn":         "  ▶  Starten  ",
        "running_btn":       "  ⏳  Läuft…  ",
        "clear_log_btn":     "  Log leeren  ",
        "delete_json_btn":   "  🗑  JSONs löschen  ",
        "backup_warning":    "⚠ Backup empfohlen! Dateien werden direkt überschrieben.",
        "ready_label":       "Bereit.",
        "done_label":        "Fertig.",
        "progress_label":    "{cur} / {tot}  ({pct:.0f}%)",
        "log_label":         "Log:",
        "dlg_no_folder":     "Bitte zuerst einen gültigen Ordner auswählen.",
        "dlg_backup_title":  "Bestätigung",
        "dlg_backup_msg":    "⚠  Dateien werden direkt überschrieben!\n\nHast du ein Backup erstellt?\n\nTrotzdem fortfahren?",
        "dlg_running_title": "Läuft",
        "dlg_running_msg":   "Bitte warten bis der aktuelle Durchlauf fertig ist.",
        "dlg_no_dir_title":  "Fehler",
        "dlg_no_dir_msg":    "Bitte zuerst einen Ordner auswählen.",
        "dlg_no_json_title": "Keine JSONs gefunden",
        "dlg_no_json_msg":   "Keine zugehörigen JSON-Dateien gefunden.",
        "dlg_del_title":     "JSONs löschen",
        "dlg_del_msg":       "{n} JSON-Sidecar-Dateien gefunden.\n\nDiese gehören zu Medien im gewählten Ordner.\n\n⚠  Diese Aktion kann nicht rückgängig gemacht werden!\n\nJetzt löschen?",
        "dlg_del_done":      "Fertig",
        "dlg_del_result":    "{n} JSON-Dateien gelöscht.",
        "dlg_del_errors":    "\n{n} Fehler (siehe Log).",
        "browse_title":      "Takeout-Ordner wählen",
        "log_exiftool_dl":   "ExifTool nicht gefunden — lade herunter…",
        "log_exiftool_fail": "\n✗  Abbruch: ExifTool konnte nicht installiert werden.",
        "log_exiftool_ok":   "✓  ExifTool: {path}",
        "log_workers":       "⚡  Parallele Worker: {n} (von {total} CPU-Kernen)",
        "log_scanning":      "📁  Durchsuche: {root}\n",
        "log_scan_json":     "Scanne JSON-Dateien…",
        "log_scan_json_ok":  "   {n} JSON-Dateien gefunden.\n",
        "log_scan_media":    "   {n} Mediendateien gefunden.\n",
        "log_preparing":     "Bereite Jobs vor…",
        "log_jobs_ready":    "   {n} Jobs vorbereitet.\n",
        "log_dry_run_file":  "  [DRY RUN] Würde verarbeiten: {rel}",
        "log_dry_run_mp":    "  [DRY RUN] Würde verarbeiten (Motion Photo Video): {rel}",
        "log_dry_no_json":   "  ⚠  Kein JSON: {rel}",
        "log_dry_summary":   "\n[DRY RUN] {n} würden verarbeitet, {m} ohne JSON.",
        "log_ok":            "  ✓  {rel}",
        "log_renamed":       "  🔄  {rel}  (war: {old})",
        "log_err":           "  ✗  {rel}\n     {msg}",
        "log_skip":          "  ⚠  Kein JSON: {rel}",
        "log_json_unread":   "  ✗  JSON unlesbar: {name}",
        "log_worker_err":    "  ✗  Worker-Fehler: {msg}",
        "log_no_json_hdr":   "\n  ── Dateien ohne JSON ({n}) ──",
        "log_summary_sep":   "═" * 60,
        "log_processed":     "  ✓  Verarbeitet:      {n}",
        "log_renamed_sum":   "  🔄  Umbenannt:        {n}",
        "log_no_json_sum":   "  ⚠  Kein JSON:        {n}",
        "log_errors_sum":    "  ✗  Fehler:           {n}",
        "log_total_sum":     "  ━  Gesamt:           {n}",
        "log_done":          "\n🎉  Fertig!",
        "log_del_ok":        "\n🗑  JSON-Cleanup: {n} gelöscht, {e} Fehler.",
        "log_del_err":       "  ✗  Fehler beim Löschen: {path}\n     {msg}",
        "dl_downloading":    "⬇  ExifTool wird heruntergeladen…",
        "dl_done":           "✓  Download abgeschlossen, entpacke…",
        "dl_installed":      "✓  ExifTool installiert: {path}",
        "dl_exe_missing":    "✗  ExifTool-Exe nicht gefunden nach dem Entpacken!",
        "dl_failed":         "✗  Download fehlgeschlagen: {e}",
        "dl_manual":         "   Bitte ExifTool manuell von https://exiftool.org herunterladen",
        "dl_manual2":        "   und exiftool.exe neben dieses Skript legen.",
    },
}

_lang = "en"  # default

def t(key: str, **kwargs) -> str:
    """Translate a string key with optional format arguments."""
    s = _STRINGS.get(_lang, _STRINGS["en"]).get(key, _STRINGS["en"].get(key, key))
    return s.format(**kwargs) if kwargs else s


# ─── Konstanten ────────────────────────────────────────────────────────────────

EXIFTOOL_URL = "https://exiftool.org/exiftool-13.30_64.zip"

def _get_base_dir() -> Path:
    """
    Gibt das Basisverzeichnis zurück — funktioniert sowohl als .py-Skript
    als auch als PyInstaller-EXE (--onefile oder --onedir).

    PyInstaller --onefile: entpackt alles nach sys._MEIPASS (temp-Ordner).
    PyInstaller --onedir:  sys._MEIPASS ist der dist-Ordner mit der exe.
    Normales Skript:       __file__ Verzeichnis.
    """
    import sys
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        # Läuft als PyInstaller-Bundle
        return Path(sys._MEIPASS)
    return Path(__file__).parent

def _get_writable_dir() -> Path:
    """
    Gibt ein beschreibbares Verzeichnis zurück für Downloads/Cache.
    Bei EXE: neben der exe-Datei (nicht im temp-MEIPASS der read-only ist).
    Bei Skript: neben dem Skript.
    """
    import sys
    if getattr(sys, "frozen", False):
        # sys.executable ist der Pfad zur .exe selbst
        return Path(sys.executable).parent
    return Path(__file__).parent

EXIFTOOL_DIR = _get_writable_dir() / "exiftool_bin"
EXIFTOOL_EXE = EXIFTOOL_DIR / "exiftool.exe"

# Alle möglichen Truncations von ".supplemental-metadata.json" (vollständig generiert)
# Google kürzt Dateinamen auf 46 Zeichen — jede Länge ist möglich.
def _generate_supplemental_suffixes():
    full = ".supplemental-metadata.json"
    seen = set()
    result = []
    for i in range(len(full), 0, -1):
        truncated = full[:i]
        # Variante 1: truncated endet bereits auf .json (z.B. echte Truncation)
        if truncated.endswith(".json") and truncated not in seen:
            seen.add(truncated)
            result.append(truncated)
        # Variante 2: truncated + ".json" (z.B. ".supplem" → ".supplem.json")
        # Nur sinnvoll wenn truncated nicht schon auf . oder .json endet
        if not truncated.endswith(".json"):  # auch ..-Varianten erlauben
            candidate = truncated + ".json"
            if candidate not in seen:
                seen.add(candidate)
                result.append(candidate)
    result.append(".json")  # altes Format (vor 2024)
    return result

SUPPLEMENTAL_SUFFIXES = _generate_supplemental_suffixes()

MEDIA_EXTENSIONS = {
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp",
    ".heic", ".heif", ".raw", ".cr2", ".nef", ".arw", ".dng",
    ".mp4", ".mov", ".avi", ".mkv", ".m4v", ".3gp", ".wmv",
    ".mp",              # Google Pixel Motion Photos (JPEG + eingebettetes Video)
    # .MP~2, .MP~3 usw.: Google Takeout nummeriert Duplikat-Videos manchmal so.
    # Häufigste Varianten hier statisch; alle weiteren via _is_mp_variant() erkannt.
    ".mp~2", ".mp~3", ".mp~4", ".mp~5",
}

def _is_mp_variant(suffix: str) -> bool:
    """True fuer .MP, .MP~2, .MP~3, ... (case-insensitive)."""
    s = suffix.lower()
    if s == ".mp":
        return True
    return bool(re.match(r"^\.mp~\d+$", s))


# ─── ExifTool Download ──────────────────────────────────────────────────────────

def find_exiftool() -> Optional[Path]:
    """
    Sucht exiftool.exe in dieser Reihenfolge:
    1. Im exiftool_bin/ Unterordner (neben Skript oder neben .exe)
    2. Im Bundle-Root (sys._MEIPASS) — wenn als PyInstaller-EXE gebündelt
    3. Im PATH des Systems
    4. Direkt neben dem Skript / der .exe
    """
    # 1. Bekannter Download-Ordner
    if EXIFTOOL_EXE.exists():
        return EXIFTOOL_EXE
    # 2. PyInstaller-Bundle: exiftool.exe wurde mit --add-binary eingebettet
    base = _get_base_dir()
    bundled = base / "exiftool.exe"
    if bundled.exists():
        return bundled
    # 3. Im PATH
    found = shutil.which("exiftool")
    if found:
        return Path(found)
    # 4. Neben Skript / exe
    local = _get_writable_dir() / "exiftool.exe"
    if local.exists():
        return local
    return None


def download_exiftool(log_fn):
    """Lädt ExifTool herunter und entpackt es."""
    log_fn(t("dl_downloading"))
    EXIFTOOL_DIR.mkdir(parents=True, exist_ok=True)
    zip_path = EXIFTOOL_DIR / "exiftool.zip"

    try:
        urllib.request.urlretrieve(EXIFTOOL_URL, zip_path)
        log_fn(t("dl_done"))
        with zipfile.ZipFile(zip_path, "r") as z:
            z.extractall(EXIFTOOL_DIR)
        zip_path.unlink()

        # Die .exe heißt nach dem Entpacken "exiftool(-k).exe" → umbenennen
        for f in EXIFTOOL_DIR.glob("exiftool*.exe"):
            f.rename(EXIFTOOL_EXE)
            break

        if EXIFTOOL_EXE.exists():
            log_fn(t("dl_installed", path=EXIFTOOL_EXE))
            return EXIFTOOL_EXE
        else:
            log_fn(t("dl_exe_missing"))
            return None
    except Exception as e:
        log_fn(t("dl_failed", e=e))
        log_fn(t("dl_manual"))
        log_fn(t("dl_manual2"))
        return None


# ─── JSON ↔ Bild Matching ──────────────────────────────────────────────────────

def strip_supplemental_suffix(filename: str) -> Optional[str]:
    """
    Entfernt den supplemental-metadata Suffix von einem JSON-Dateinamen.
    Gibt den Basisnamen zurück (z.B. 'IMG_1234.jpg') oder None.
    """
    name = filename
    for suffix in SUPPLEMENTAL_SUFFIXES:
        if name.endswith(suffix):
            base = name[: -len(suffix)]
            return base
    return None


# Medien-Erweiterungen als Unterstrich-Variante (Google-Bug: .jpg → _jpg_)
_MEDIA_EXTS_FOR_UNDERSCORE = [
    "jpg", "jpeg", "png", "gif", "bmp", "tiff", "tif", "webp",
    "heic", "heif", "mp4", "mov", "avi", "mkv", "m4v", "3gp", "wmv",
    "raw", "cr2", "nef", "arw", "dng",
]

def _normalize(s: str) -> str:
    """Leerzeichen und Unterstriche als äquivalent behandeln, alles lowercase."""
    return s.lower().replace(" ", "_")


def strip_underscore_ext(filename: str) -> Optional[str]:
    """
    Behandelt Google-Takeout-Bug: Punkt vor Dateiendung wird zu Unterstrich.
    z.B. Screenshot_Discord_jpg_supplem.json → Screenshot_Discord.jpg
    Gibt den rekonstruierten Mediendateinamen zurück oder None.
    """
    name_lower = filename.lower()
    for ext in _MEDIA_EXTS_FOR_UNDERSCORE:
        # Suche _{ext}_ gefolgt von supplem... oder anderen JSON-Suffixen
        idx = name_lower.rfind(f"_{ext}_")
        if idx == -1:
            continue
        # Prüfe ob danach ein bekannter JSON-Suffix-Anfang kommt
        rest = name_lower[idx + len(ext) + 2:]  # nach _{ext}_
        if (rest.startswith("supplem") or rest.startswith("s.json") or
                rest.endswith(".json") or rest == ""):
            original = filename[:idx] + f".{ext}"
            return original
    return None


def build_json_map(root: Path) -> tuple:
    """
    Baut zwei Datenstrukturen für schnelles JSON-Matching:
      - json_map:    (ordner, normalisierter_name) → json_pfad  (exakter Treffer, O(1))
      - prefix_map:  ordner → [(basis_norm, json_pfad), ...]    (Präfix-Fallback, O(k))
    k = Anzahl JSONs im selben Ordner, statt O(gesamt).

    Sonderfall Motion Photos (Google Pixel .MP):
      Die JSON-Datei heißt z.B. "PXL_20220622.MP.jpg.json" (base = "PXL_20220622.MP.jpg").
      Das dazugehörige Video heißt nur "PXL_20220622.MP" (ohne .jpg).
      Deshalb wird der base ZUSÄTZLICH ohne seine letzte Extension eingetragen,
      damit das Video-File einen direkten Treffer bekommt.
    """
    json_map: dict = {}
    prefix_map: dict = {}  # ordner → liste von (basis_norm, json_pfad)

    for json_file in root.rglob("*.json"):
        folder = str(json_file.parent)
        base = strip_supplemental_suffix(json_file.name)
        if base is None:
            base = strip_underscore_ext(json_file.name)
        if base is None:
            continue

        base_norm = _normalize(base)
        key = (folder, base_norm)
        if key not in json_map:
            json_map[key] = json_file

        # ── Motion-Photo-Sonderfall ───────────────────────────────────────────
        # JSON-Base endet auf ".mp.jpg" (oder ".mp.jpeg") → Video-Datei hat nur ".mp"
        # Auch .MP~2, .MP~3-Varianten bekommen einen Zusatz-Schlüssel.
        base_path = Path(base)
        if base_path.suffix.lower() in (".jpg", ".jpeg"):
            inner_stem = base_path.stem   # z.B. "PXL_20220622_114604997.MP"
            if _is_mp_variant(Path(inner_stem).suffix):
                # Eintrag für .MP (ohne .jpg)
                alt_key = (folder, _normalize(inner_stem))
                if alt_key not in json_map:
                    json_map[alt_key] = json_file
                # Einträge für .MP~2, .MP~3 ... ~9 (falls Google so exportiert)
                mp_base = inner_stem  # z.B. "PXL...MP"
                for n in range(2, 10):
                    variant_key = (folder, _normalize(f"{mp_base}~{n}"))
                    if variant_key not in json_map:
                        json_map[variant_key] = json_file

        # Für Präfix-Matching: immer eintragen — find_json_for_media
        # prüft selbst ob es ein echter Präfix-Match ist (ratio + startswith)
        if folder not in prefix_map:
            prefix_map[folder] = []
        prefix_map[folder].append((base_norm, json_file))

    return json_map, prefix_map


def find_json_for_media(media_path: Path, json_map: dict,
                        prefix_map: Optional[dict] = None) -> Optional[Path]:
    """
    Findet die passende JSON-Datei für eine Mediendatei.
    json_map:   exakter O(1) Lookup
    prefix_map: Präfix-Fallback, nur Einträge im gleichen Ordner → O(k)
    """
    folder = str(media_path.parent)
    name = media_path.name

    # Direkter Treffer (normalisiert: Leerzeichen == Unterstrich)
    key = (folder, _normalize(name))
    if key in json_map:
        return json_map[key]

    # ── Motion-Photo-Sonderfall: .MP/.MP~N-Video sucht JSON der .MP.jpg ─────
    # z.B. "PXL...MP~2" → probiere "PXL...MP.jpg", "PXL...MP~2.jpg"
    if _is_mp_variant(media_path.suffix):
        for photo_ext in (".jpg", ".jpeg"):
            # direkte Variante: name + .jpg (z.B. "PXL...MP~2.jpg")
            alt_key = (folder, _normalize(name + photo_ext))
            if alt_key in json_map:
                return json_map[alt_key]
        # Für .MP~N: auch die Basis ohne ~N probieren → "PXL...MP.jpg"
        if media_path.suffix.lower() != ".mp":
            # Stem enthält schon ".MP", suffix ist "~N" — nein:
            # Path("PXL...MP~2").suffix == "~2", stem == "PXL...MP"
            mp_base_name = media_path.stem  # z.B. "PXL_20220622_114604997.MP"
            for photo_ext in (".jpg", ".jpeg"):
                alt_key2 = (folder, _normalize(mp_base_name + photo_ext))
                if alt_key2 in json_map:
                    return json_map[alt_key2]
            # Auch direkt als .MP (ohne ~N)
            alt_key3 = (folder, _normalize(mp_base_name))
            if alt_key3 in json_map:
                return json_map[alt_key3]

    # Duplikat-Nummerierung entfernen: IMG(1).jpg → IMG.jpg
    stem = media_path.stem
    ext = media_path.suffix
    dedup_match = re.match(r"^(.*?)\s*\((\d+)\)$", stem)
    if dedup_match:
        base_stem = dedup_match.group(1)
        num = dedup_match.group(2)
        for suffix in SUPPLEMENTAL_SUFFIXES:
            candidate = Path(folder) / f"{base_stem}{ext}({num}){suffix}"
            if candidate.exists():
                return candidate
        key2 = (folder, _normalize(f"{base_stem}{ext}"))
        if key2 in json_map:
            return json_map[key2]
        # Sonderfall: cropped Motion Photo, z.B. "PXL...MP(1).jpg" → JSON von "PXL...MP.jpg"
        if ext.lower() in (".jpg", ".jpeg"):
            key2b = (folder, _normalize(f"{base_stem}{ext}"))
            if key2b in json_map:
                return json_map[key2b]

    # -edited / -bearbeitet / andere Sprachen / crop-Varianten
    for edited_suffix in [
        "-edited", "-cropped", "-effects", "-smile", "-mix",
        "-bearbeitet", "-modifié", "-modifie", "-editado",
        "-modificato", "-bewerkt", "-edytowane",
    ]:
        if stem.lower().endswith(edited_suffix):
            original_stem = stem[: -len(edited_suffix)]
            key3 = (folder, _normalize(f"{original_stem}{ext}"))
            if key3 in json_map:
                return json_map[key3]

    # ── Fallback: Präfix-Matching (O(k) statt O(gesamt)) ─────────────────────
    # Nur Einträge im gleichen Ordner prüfen → massiv schneller
    MIN_PREFIX_RATIO = 0.85
    media_stem_norm = _normalize(media_path.stem)
    name_norm = _normalize(name)
    candidates = (prefix_map or {}).get(folder, [])
    best_json = None
    best_ratio = MIN_PREFIX_RATIO - 0.001
    for jbase, jpath in candidates:
        if not name_norm.startswith(jbase):
            continue
        # Ratio gegen Stem ODER vollen Namen — nimm den größeren (konservativeren)
        # Wenn JSON-Base eine teilweise Extension enthält (.jp statt .jpg),
        # ist der Stem zu kurz als Referenz → vollen Namen verwenden
        ref_len = max(len(media_stem_norm), len(name_norm) - 1)
        ratio = len(jbase) / ref_len if ref_len else 0
        if ratio > best_ratio:
            best_ratio = ratio
            best_json = jpath
    if best_json:
        return best_json

    # ── Letzter Fallback für .MP/.MP~N: JSON-Base als Präfix des Media-Namens ─
    # Umgekehrte Richtung: JSON-Base (z.B. "PXL...MP.jpg") ist LÄNGER als
    # Media-Name (z.B. "PXL...MP") → normales startswith schlägt fehl.
    if _is_mp_variant(media_path.suffix):
        # Für .MP~N: stem ist z.B. "PXL...MP", für .MP: stem ist "PXL...114604997"
        # Wir nehmen den stem ohne die ~N-Variante
        if media_path.suffix.lower() == ".mp":
            stem_norm = _normalize(media_path.stem)
        else:
            stem_norm = _normalize(media_path.stem)  # stem enthält schon .MP
        for jbase, jpath in candidates:
            if jbase.startswith(stem_norm + "."):
                return jpath

    return None



# ─── Metadaten Merge ───────────────────────────────────────────────────────────

def read_json_metadata(json_path: Path) -> dict:
    """Liest und validiert eine Google-Takeout-JSON-Datei."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception:
        return {}


def detect_real_filetype(media_path: Path) -> str:
    """
    Liest die Magic Bytes einer Datei und gibt den echten Typ zurück.
    Google speichert manchmal JPEGs mit .png Endung.
    Gibt die echte Extension zurück (z.B. ".jpg" statt ".png").
    """
    try:
        with open(media_path, "rb") as f:
            header = f.read(12)
        if header[:3] == b"\xff\xd8\xff":
            return ".jpg"   # JPEG magic bytes
        if header[:8] == b"\x89PNG\r\n\x1a\n":
            return ".png"   # echtes PNG
        if header[:4] in (b"ftyp", b"\x00\x00\x00\x18", b"\x00\x00\x00\x20") or header[4:8] == b"ftyp":
            return ".mp4"
        if header[:4] == b"RIFF":
            return ".avi"
    except Exception:
        pass
    return media_path.suffix.lower()  # Fallback: Extension vertrauen


def build_exiftool_args(media_path: Path, meta: dict, exiftool: Path) -> list:
    """
    Erstellt die ExifTool-Argumente für eine Datei.
    Schreibt dateityp-spezifische Tags:
      - JPEG/HEIC: EXIF AllDates + XMP
      - PNG (echt): nur XMP
      - Video:      QuickTime-Tags
      - Falsche Extension (JPEG mit .png): needs_rename=True → Worker benennt um
    I/O-Optimierung: Magic-Bytes werden nur für .png Dateien gelesen,
    da nur Google-PNG-Dateien falsche Extensions bekommen können.
    """
    ext = media_path.suffix.lower()
    # .mp = Google Motion Photo (JPEG-basiert, nicht als Video behandeln)
    is_video = ext in {".mp4", ".mov", ".avi", ".mkv", ".m4v", ".3gp", ".wmv"}
    needs_rename = False

    if ext == ".png":
        # Nur für PNG: Magic Bytes lesen (1 extra Read, aber nötig)
        real_ext = detect_real_filetype(media_path)
        is_png  = (real_ext == ".png")
        needs_rename = (real_ext != ".png" and real_ext in {".jpg", ".jpeg"})
    else:
        is_png = False

    if is_png:
        args = [str(exiftool), "-overwrite_original"]
    else:
        args = [str(exiftool), "-overwrite_original", "-m"]

    # ── Datum ──────────────────────────────────────────────────────────────────
    photo_taken = meta.get("photoTakenTime", {})
    timestamp = photo_taken.get("timestamp")
    if timestamp:
        try:
            ts = int(timestamp)
            from datetime import datetime, timezone
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            dt_str = dt.strftime("%Y:%m:%d %H:%M:%S")
            dt_str_tz = dt.strftime("%Y:%m:%d %H:%M:%S+00:00")

            if is_video:
                args += [
                    f"-QuickTime:CreateDate={dt_str}",
                    f"-QuickTime:ModifyDate={dt_str}",
                    f"-QuickTime:TrackCreateDate={dt_str}",
                    f"-QuickTime:TrackModifyDate={dt_str}",
                    f"-QuickTime:MediaCreateDate={dt_str}",
                    f"-QuickTime:MediaModifyDate={dt_str}",
                ]
            elif is_png:
                # PNG: XMP ist der einzige portable Standard.
                # -EXIF:DateTimeOriginal würde "non-standard EXIF in PNG" Warning erzeugen
                # was mit -m unterdrückt wird → ExifTool schreibt 0 files updated.
                # Lösung: nur XMP schreiben, kein -m für PNG nötig.
                args += [
                    f"-XMP:DateTimeOriginal={dt_str_tz}",
                    f"-XMP:CreateDate={dt_str_tz}",
                    f"-XMP:ModifyDate={dt_str_tz}",
                ]
            else:
                # JPEG, HEIC und alles andere mit EXIF-Support
                args += [
                    f"-AllDates={dt_str}",
                    f"-DateTimeOriginal={dt_str}",
                    f"-CreateDate={dt_str}",
                    f"-ModifyDate={dt_str}",
                    f"-XMP:DateTimeOriginal={dt_str_tz}",
                ]
            # Dateisystem-Zeitstempel immer setzen
            args += [f"-FileModifyDate={dt_str_tz}"]
        except (ValueError, OSError):
            pass

    # ── GPS ────────────────────────────────────────────────────────────────────
    geo = meta.get("geoData", {})
    lat = geo.get("latitude", 0.0)
    lon = geo.get("longitude", 0.0)
    alt = geo.get("altitude", 0.0)

    if lat != 0.0 or lon != 0.0:
        args += [
            f"-GPSLatitude={abs(lat)}",
            f"-GPSLatitudeRef={'N' if lat >= 0 else 'S'}",
            f"-GPSLongitude={abs(lon)}",
            f"-GPSLongitudeRef={'E' if lon >= 0 else 'W'}",
            f"-GPSAltitude={abs(alt)}",
            f"-GPSAltitudeRef={'0' if alt >= 0 else '1'}",
        ]

    # ── Beschreibung ───────────────────────────────────────────────────────────
    description = meta.get("description", "").strip()
    if description:
        args += [
            f"-ImageDescription={description}",
            f"-XMP:Description={description}",
            f"-IPTC:Caption-Abstract={description}",
        ]

    # ── Titel ──────────────────────────────────────────────────────────────────
    title = meta.get("title", "").strip()
    if title and title != media_path.name:
        args += [f"-XMP:Title={title}"]

    args.append(str(media_path))
    return args, needs_rename


# ─── ExifTool Persistent Worker ───────────────────────────────────────────────

class ExifToolWorker:
    """
    Hält einen ExifTool-Prozess dauerhaft offen (-stay_open Modus).
    Eliminiert den Prozess-Start-Overhead: statt N Prozesse nur 1 pro Worker.
    Typisch 10-50x schneller als einzelne subprocess-Aufrufe.
    """
    def __init__(self, exiftool_path: Path):
        self.proc = subprocess.Popen(
            [str(exiftool_path), "-stay_open", "True", "-@", "-"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,   # separat von stdout halten
            text=True,
            bufsize=1,                # line-buffered (kompatibel mit text=True)
        )
        # stderr-Zeilen in Queue sammeln (nicht verwerfen!)
        # so können Fehlermeldungen im execute() abgerufen werden
        import threading, queue as _q
        self._stderr_q = _q.Queue()
        def _collect_stderr():
            try:
                for line in self.proc.stderr:
                    self._stderr_q.put(line.rstrip())
            except Exception:
                pass
        threading.Thread(target=_collect_stderr, daemon=True).start()

    def execute(self, args: list) -> tuple:
        """
        Schickt Argumente an den laufenden ExifTool-Prozess.
        args: vollständige Liste inkl. exiftool-Pfad als args[0] und Dateipfad am Ende.
        Gibt (success, output) zurück.
        """
        # -echo4 schreibt einen eindeutigen Sentinel auf stdout nach {ready}
        # so dass wir sicher wissen wann die Ausgabe komplett ist
        cmd = "\n".join(args[1:]) + "\n-execute\n"
        try:
            self.proc.stdin.write(cmd)
            self.proc.stdin.flush()
        except (BrokenPipeError, OSError):
            return False, "ExifTool-Prozess unerwartet beendet"

        out_lines = []
        while True:
            line = self.proc.stdout.readline()
            if not line:  # Prozess tot
                return False, "ExifTool-Prozess unerwartet beendet"
            stripped = line.strip()
            if stripped == "{ready}":
                break
            if stripped:
                out_lines.append(stripped)

        output = "\n".join(out_lines).strip()

        # Stderr-Zeilen holen (nicht blockierend)
        import re as _re, queue as _q
        stderr_lines = []
        try:
            while True:
                stderr_lines.append(self._stderr_q.get_nowait())
        except _q.Empty:
            pass
        if stderr_lines:
            output = output + ("\n" if output else "") + "\n".join(stderr_lines)

        # Robuster Erfolgs-Check
        updated_match = _re.search(r"(\d+) (?:image|video) files? updated", output)
        if updated_match and int(updated_match.group(1)) > 0:
            return True, output
        if not output or "unchanged" in output.lower():
            return True, output
        lines = [l for l in output.splitlines() if l.strip()]
        only_warnings = all(
            l.strip().lower().startswith("warning")
            for l in lines if l.strip()
        )
        if only_warnings and lines:
            return True, output
        return False, output

    def close(self):
        try:
            self.proc.stdin.write("-stay_open\nFalse\n")
            self.proc.stdin.flush()
            self.proc.wait(timeout=5)
        except Exception:
            self.proc.kill()


def _process_chunk(chunk_args):
    """Worker-Funktion für einen Thread: eigener ExifTool-Prozess pro Worker."""
    exiftool_path, jobs = chunk_args
    results = []
    worker = ExifToolWorker(exiftool_path)
    try:
        for rel, et_args in jobs:
            try:
                ok, output = worker.execute(et_args)
                results.append(("ok" if ok else "err", rel, output))
            except Exception as e:
                results.append(("err", rel, str(e)))
    finally:
        worker.close()
    return results


# ─── Haupt-Worker ─────────────────────────────────────────────────────────────

def process_takeout(root_dir: str, log_fn, progress_fn, dry_run: bool = False,
                    num_workers: int = None):
    """
    Hauptfunktion: Durchsucht alle Unterordner, matched JSON ↔ Medien,
    ruft ExifTool parallel im Persistent-Mode auf.
    num_workers: Anzahl paralleler ExifTool-Prozesse (Standard: 50% der CPU-Kerne)
    """
    import queue as _queue

    root = Path(root_dir)

    # ExifTool prüfen / herunterladen
    exiftool = find_exiftool()
    if not exiftool:
        log_fn(t("log_exiftool_dl"))
        exiftool = download_exiftool(log_fn)
        if not exiftool:
            log_fn(t("log_exiftool_fail"))
            return

    # Anzahl Worker: Standard = 2 (HDD-freundlich), mind. 1, max. 8
    if num_workers is None:
        # HDD-freundlich: 2 Worker Standard. Mehr Worker = mehr parallele
        # Schreibzugriffe = I/O-Sättigung auf normalen Festplatten.
        # Für SSDs kann man 4-6 einstellen.
        num_workers = 2

    log_fn(t("log_exiftool_ok", path=exiftool))
    log_fn(t("log_workers", n=num_workers, total=os.cpu_count()))
    log_fn(t("log_scanning", root=root))

    # JSON-Map aufbauen
    log_fn(t("log_scan_json"))
    json_map, prefix_map = build_json_map(root)
    log_fn(t("log_scan_json_ok", n=len(json_map)))

    # Alle Mediendateien finden (os.walk ist schneller als rglob auf Windows)
    media_files = []
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            suf = Path(fname).suffix.lower()
            if suf in MEDIA_EXTENSIONS or _is_mp_variant(suf):
                media_files.append(Path(dirpath) / fname)
    total = len(media_files)
    log_fn(t("log_scan_media", n=total))

    stats = {"processed": 0, "no_json": 0, "errors": 0, "renamed": 0}
    no_json_files = []

    if dry_run:
        for media in media_files:
            json_file = find_json_for_media(media, json_map, prefix_map)
            if not json_file:
                stats["no_json"] += 1
                log_fn(t("log_dry_no_json", rel=media.relative_to(root)))
            else:
                rel = media.relative_to(root)
                if _is_mp_variant(media.suffix):
                    log_fn(t("log_dry_run_mp", rel=rel))
                else:
                    log_fn(t("log_dry_run_file", rel=rel))
                stats["processed"] += 1
        log_fn(t("log_dry_summary", n=stats["processed"], m=stats["no_json"]))
        return

    # ── Producer-Consumer mit Result-Queue ────────────────────────────────────
    # job_queue:    Producer → Worker   (Jobs zum Verarbeiten)
    # result_queue: Worker  → Main      (Ergebnisse sofort, nicht gesammelt)
    # So updated der Fortschrittsbalken nach jeder einzelnen Datei.
    job_queue    = _queue.Queue(maxsize=num_workers * 20)
    result_queue = _queue.Queue()
    completed = 0

    def _producer():
        for media in media_files:
            json_file = find_json_for_media(media, json_map, prefix_map)
            if not json_file:
                no_json_files.append(str(media.relative_to(root)))
                result_queue.put(("skip", str(media.relative_to(root)), "", None))
                continue
            meta = read_json_metadata(json_file)
            if not meta:
                result_queue.put(("err", str(media.relative_to(root)), t("log_json_unread", name=json_file.name), None))
                continue
            et_args, needs_rename = build_exiftool_args(media, meta, exiftool)
            job_queue.put((str(media.relative_to(root)), str(media),
                           str(json_file), et_args, needs_rename))
        for _ in range(num_workers):
            job_queue.put("DONE")

    def _worker():
        w = ExifToolWorker(exiftool)
        try:
            while True:
                item = job_queue.get()
                if item == "DONE":
                    break
                rel, abs_path, json_path, et_args, needs_rename = item
                try:
                    renamed_from = None
                    if needs_rename:
                        # Datei hat falsche Extension (JPEG mit .png) →
                        # ExifTool hat keinen Flag dafür, Umbenennen ist einzige Lösung.
                        old_path = Path(abs_path)
                        real_ext = detect_real_filetype(old_path)
                        new_media = old_path.with_suffix(real_ext)
                        if new_media.exists():
                            new_media = old_path.with_suffix(f".renamed{real_ext}")
                        old_path.rename(new_media)
                        renamed_from = old_path.name   # für Log
                        # JSON umbenennen (Konsistenz)
                        # Direkt den übergebenen json_path verwenden — NICHT
                        # den Namen rekonstruieren, da JSON-Namen truncated sein können
                        if json_path and Path(json_path).exists():
                            old_json = Path(json_path)
                            # Ersetze nur die Medien-Extension im JSON-Namen
                            # z.B. "foto.png.supplemental-me.json"
                            #   → "foto.jpg.supplemental-me.json"
                            old_suffix = old_path.suffix  # ".png"
                            new_json_name = old_json.name.replace(
                                old_suffix, real_ext, 1)  # nur erste Stelle
                            new_json = old_json.with_name(new_json_name)
                            if not new_json.exists():
                                try:
                                    old_json.rename(new_json)
                                except OSError:
                                    pass  # JSON-Rename optional, kein fataler Fehler
                        # et_args aktualisieren
                        et_args = et_args[:-1] + [str(new_media)]
                        rel = str(Path(rel).with_suffix(real_ext))
                    ok, output = w.execute(et_args)
                    status = "renamed_ok" if (ok and renamed_from) else \
                             "renamed_err" if (not ok and renamed_from) else \
                             "ok" if ok else "err"
                    result_queue.put((status, rel, output, renamed_from))
                except Exception as e:
                    result_queue.put(("err", rel, str(e), None))
        finally:
            w.close()
            result_queue.put("WORKER_DONE")

    # Starte Producer + Worker-Threads
    producer_thread = threading.Thread(target=_producer, daemon=True)
    producer_thread.start()

    worker_threads = []
    for _ in range(num_workers):
        _th = threading.Thread(target=_worker, daemon=True)
        _th.start()
        worker_threads.append(_th)

    # Main-Thread: liest Result-Queue sofort → GUI updated nach jeder Datei
    workers_done = 0
    while workers_done < num_workers:
        try:
            item = result_queue.get(timeout=0.1)
        except _queue.Empty:
            continue
        if item == "WORKER_DONE":
            workers_done += 1
            continue
        status, rel, output, renamed_from = item
        completed += 1
        progress_fn(completed, total)
        if status == "skip":
            stats["no_json"] += 1
        elif status in ("ok", "renamed_ok"):
            stats["processed"] += 1
            if status == "renamed_ok":
                stats["renamed"] += 1
                log_fn(t("log_renamed", rel=rel, old=renamed_from))
            else:
                log_fn(t("log_ok", rel=rel))
        else:
            stats["errors"] += 1
            log_fn(t("log_err", rel=rel, msg=output))

    producer_thread.join()
    for _th in worker_threads:
        _th.join()

    # Kein-JSON Dateien am Ende loggen
    if no_json_files:
        log_fn(t("log_no_json_hdr", n=len(no_json_files)))
        for f in no_json_files:
            log_fn(t("log_skip", rel=f))

    log_fn("\n" + t("log_summary_sep"))
    log_fn(t("log_processed", n=stats["processed"]))
    log_fn(t("log_renamed_sum", n=stats["renamed"]))
    log_fn(t("log_no_json_sum", n=stats["no_json"]))
    log_fn(t("log_errors_sum", n=stats["errors"]))
    log_fn(t("log_total_sum", n=len(media_files)))
    log_fn(t("log_summary_sep"))
    log_fn(t("log_done"))

# ─── GUI ──────────────────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title(t("window_title"))
        self.geometry("820x620")
        self.resizable(True, True)
        self.configure(bg="#1a1a2e")
        self._build_ui()
        self._running = False

    def _build_ui(self):
        BG = "#1a1a2e"
        PANEL = "#16213e"
        ACCENT = "#0f3460"
        GREEN = "#00d4aa"
        TEXT = "#e0e0e0"
        MUTED = "#888"
        FONT = ("Segoe UI", 10)
        FONT_BOLD = ("Segoe UI", 10, "bold")
        FONT_TITLE = ("Segoe UI", 14, "bold")

        # ── Title + language toggle ────────────────────────────────────────────
        title_frame = tk.Frame(self, bg=BG, pady=16)
        title_frame.pack(fill="x", padx=20)
        self.title_label = tk.Label(
            title_frame,
            text=t("app_title"),
            font=FONT_TITLE,
            bg=BG,
            fg=GREEN,
        )
        self.title_label.pack(side="left")
        self.version_label = tk.Label(
            title_frame,
            text=t("app_version"),
            font=("Segoe UI", 9),
            bg=BG,
            fg=MUTED,
        )
        self.version_label.pack(side="left", padx=(8, 0), pady=(4, 0))
        # Language toggle button (right side)
        self.lang_btn = tk.Button(
            title_frame,
            text="🌐 DE",
            font=("Segoe UI", 8),
            bg=ACCENT, fg=TEXT,
            activebackground="#1a4080",
            relief="flat", cursor="hand2",
            padx=6, pady=2,
            command=self._toggle_lang,
        )
        self.lang_btn.pack(side="right")

        # ── Ordner-Auswahl ────────────────────────────────────────────────────
        dir_frame = tk.Frame(self, bg=PANEL, bd=0, padx=16, pady=12)
        dir_frame.pack(fill="x", padx=20, pady=(0, 8))

        self.folder_label = tk.Label(dir_frame, text=t("folder_label"), font=FONT_BOLD, bg=PANEL, fg=TEXT)
        self.folder_label.pack(anchor="w")
        row = tk.Frame(dir_frame, bg=PANEL)
        row.pack(fill="x", pady=(4, 0))

        self.dir_var = tk.StringVar()
        tk.Entry(
            row,
            textvariable=self.dir_var,
            font=FONT,
            bg=ACCENT,
            fg=TEXT,
            insertbackground=GREEN,
            relief="flat",
            bd=6,
        ).pack(side="left", fill="x", expand=True)

        tk.Button(
            row,
            text=t("browse_btn"),
            font=FONT,
            bg=GREEN,
            fg="#1a1a2e",
            activebackground="#00b894",
            relief="flat",
            cursor="hand2",
            command=self._browse,
        ).pack(side="left", padx=(8, 0))

        # ── Optionen ──────────────────────────────────────────────────────────
        opt_frame = tk.Frame(self, bg=BG, padx=20)
        opt_frame.pack(fill="x")

        self.dry_run_var = tk.BooleanVar(value=False)
        self.dry_run_cb = tk.Checkbutton(
            opt_frame,
            text=t("dry_run_label"),
            variable=self.dry_run_var,
            font=FONT,
            bg=BG,
            fg=TEXT,
            selectcolor=ACCENT,
            activebackground=BG,
            activeforeground=GREEN,
            cursor="hand2",
        )
        self.dry_run_cb.pack(side="left", pady=6)

        # ── Worker-Slider ─────────────────────────────────────────────────────
        worker_frame = tk.Frame(self, bg=BG, padx=20)
        worker_frame.pack(fill="x")

        cpu_max = os.cpu_count() or 2
        # Standard: 2 Worker — gut für HDD und SSD
        default_workers = 2
        self.workers_var = tk.IntVar(value=default_workers)

        self.workers_txt_label = tk.Label(worker_frame, text=t("workers_label_txt"), font=FONT, bg=BG, fg=TEXT)
        self.workers_txt_label.pack(side="left")
        self.workers_label = tk.Label(
            worker_frame, text=str(default_workers), font=FONT_BOLD, bg=BG, fg=GREEN, width=3
        )
        self.workers_label.pack(side="left", padx=(4, 8))

        self.workers_slider = tk.Scale(
            worker_frame,
            from_=1, to=min(cpu_max, 8),
            orient="horizontal",
            variable=self.workers_var,
            bg=BG, fg=TEXT, troughcolor=ACCENT,
            highlightthickness=0, relief="flat", sliderlength=16,
            showvalue=False, length=160,
            command=lambda v: self.workers_label.config(text=str(int(float(v)))),
        )
        self.workers_slider.pack(side="left")
        self.workers_lock_label = tk.Label(
            worker_frame, text="", font=("Segoe UI", 10),
            bg=BG, fg="#cc6600"
        )
        self.workers_lock_label.pack(side="left", padx=(4, 0))
        self.workers_hint_label = tk.Label(
            worker_frame,
            text=t("workers_hint", n=min(cpu_max, 8)),
            font=("Segoe UI", 8), bg=BG, fg=MUTED
        )
        self.workers_hint_label.pack(side="left", padx=(4, 0))

        # ── Fortschrittsbalken ────────────────────────────────────────────────
        prog_frame = tk.Frame(self, bg=BG, padx=20, pady=4)
        prog_frame.pack(fill="x")

        self.progress_label = tk.Label(
            prog_frame, text=t("ready_label"), font=("Segoe UI", 9), bg=BG, fg=MUTED
        )
        self.progress_label.pack(anchor="w")

        self.progress = ttk.Progressbar(prog_frame, mode="determinate", length=200)
        style = ttk.Style()
        style.theme_use("clam")
        style.configure(
            "green.Horizontal.TProgressbar",
            troughcolor=ACCENT,
            background=GREEN,
            bordercolor=PANEL,
            lightcolor=GREEN,
            darkcolor=GREEN,
        )
        self.progress.configure(style="green.Horizontal.TProgressbar")
        self.progress.pack(fill="x", pady=(2, 0))

        # ── Buttons (VOR dem Log gepackt mit side="bottom", immer sichtbar) ──
        btn_frame = tk.Frame(self, bg=BG, padx=20, pady=8)
        btn_frame.pack(fill="x", side="bottom")

        self.start_btn = tk.Button(
            btn_frame,
            text=t("start_btn"),
            font=FONT_BOLD,
            bg=GREEN,
            fg="#1a1a2e",
            activebackground="#00b894",
            relief="flat",
            cursor="hand2",
            command=self._start,
            padx=12,
            pady=6,
        )
        self.start_btn.pack(side="left")

        tk.Button(
            btn_frame,
            text=t("clear_log_btn"),
            font=FONT,
            bg=ACCENT,
            fg=TEXT,
            activebackground="#1a4080",
            relief="flat",
            cursor="hand2",
            command=self._clear_log,
            padx=8,
            pady=6,
        ).pack(side="left", padx=(8, 0))

        tk.Button(
            btn_frame,
            text=t("delete_json_btn"),
            font=FONT,
            bg="#3d1a1a",
            fg="#ff6b6b",
            activebackground="#5a2020",
            relief="flat",
            cursor="hand2",
            command=self._delete_jsons,
            padx=8,
            pady=6,
        ).pack(side="left", padx=(8, 0))

        self.backup_warn_label = tk.Label(
            btn_frame,
            text=t("backup_warning"),
            font=("Segoe UI", 8),
            bg=BG,
            fg="#cc6600",
        )
        self.backup_warn_label.pack(side="right")

        # ── Log-Fenster (zuletzt, füllt restlichen Platz) ─────────────────────
        log_frame = tk.Frame(self, bg=BG, padx=20, pady=4)
        log_frame.pack(fill="both", expand=True)

        self.log_label = tk.Label(log_frame, text=t("log_label"), font=FONT_BOLD, bg=BG, fg=MUTED)
        self.log_label.pack(anchor="w")

        # Custom scrollbar to match theme (ScrolledText uses a default scrollbar)
        log_inner = tk.Frame(log_frame, bg="#0d0d1a")
        log_inner.pack(fill="both", expand=True)
        log_inner.rowconfigure(0, weight=1)
        log_inner.columnconfigure(0, weight=1)

        self.log = tk.Text(
            log_inner,
            font=("Consolas", 9),
            bg="#0d0d1a",
            fg=TEXT,
            insertbackground=GREEN,
            relief="flat",
            state="disabled",
            wrap="word",
            yscrollcommand=lambda *a: _log_vsb.set(*a),
        )
        self.log.grid(row=0, column=0, sticky="nsew")

        style = ttk.Style()
        style.theme_use("clam")
        style.configure("LogScroll.Vertical.TScrollbar",
            background="#1e1e3a",      # thumb colour
            troughcolor="#0d0d1a",     # matches log bg
            bordercolor="#0d0d1a",
            arrowcolor="#00d4aa",
            darkcolor="#1e1e3a",
            lightcolor="#1e1e3a",
            relief="flat",
        )
        style.map("LogScroll.Vertical.TScrollbar",
            background=[("active", "#00d4aa"), ("pressed", "#00b894")],
        )
        _log_vsb = ttk.Scrollbar(log_inner, orient="vertical",
                                  style="LogScroll.Vertical.TScrollbar",
                                  command=self.log.yview)
        _log_vsb.grid(row=0, column=1, sticky="ns")

    def _toggle_lang(self):
        global _lang
        _lang = "de" if _lang == "en" else "en"
        self.lang_btn.configure(text="🌐 DE" if _lang == "en" else "🌐 EN")
        self._refresh_ui()

    def _refresh_ui(self):
        """Update all translatable UI widgets after language change."""
        cpu_max = os.cpu_count() or 2
        self.title(t("window_title"))
        self.title_label.configure(text=t("app_title"))
        self.version_label.configure(text=t("app_version"))
        self.folder_label.configure(text=t("folder_label"))
        self.dry_run_cb.configure(text=t("dry_run_label"))
        self.workers_txt_label.configure(text=t("workers_label_txt"))
        self.workers_hint_label.configure(text=t("workers_hint", n=min(cpu_max, 8)))
        self.start_btn.configure(
            text=t("running_btn") if self._running else t("start_btn")
        )
        self.backup_warn_label.configure(text=t("backup_warning"))
        self.log_label.configure(text=t("log_label"))
        if not self._running:
            self.progress_label.configure(text=t("ready_label"))

    def _browse(self):
        d = filedialog.askdirectory(title=t("browse_title"))
        if d:
            self.dir_var.set(d)

    def _delete_jsons(self):
        """Löscht alle JSON-Sidecar-Dateien die zu Medien gehören."""
        if self._running:
            messagebox.showwarning(t("dlg_running_title"), t("dlg_running_msg"))
            return
        d = self.dir_var.get().strip()
        if not d or not os.path.isdir(d):
            messagebox.showerror(t("dlg_no_dir_title"), t("dlg_no_dir_msg"))
            return

        # Zuerst zählen ohne zu löschen
        root = Path(d)
        json_map, _ = build_json_map(root)
        to_delete = list({str(p) for p in json_map.values()})

        if not to_delete:
            messagebox.showinfo(t("dlg_no_json_title"), t("dlg_no_json_msg"))
            return

        msg = t("dlg_del_msg", n=len(to_delete))
        if not messagebox.askyesno(t("dlg_del_title"), msg):
            return

        deleted = 0
        errors = 0
        for path in to_delete:
            try:
                os.remove(path)
                deleted += 1
            except Exception as e:
                errors += 1
                self._log(t("log_del_err", path=path, msg=e))

        self._log(t("log_del_ok", n=deleted, e=errors))
        messagebox.showinfo(
            t("dlg_del_done"),
            t("dlg_del_result", n=deleted)
            + (t("dlg_del_errors", n=errors) if errors else ""),
        )

    def _log(self, msg: str):
        self.log.configure(state="normal")
        self.log.insert("end", msg + "\n")
        self.log.see("end")
        self.log.configure(state="disabled")

    def _clear_log(self):
        self.log.configure(state="normal")
        self.log.delete("1.0", "end")
        self.log.configure(state="disabled")

    def _progress(self, current: int, total: int):
        pct = (current / total * 100) if total else 0
        self.progress["value"] = pct
        self.progress_label.configure(text=t("progress_label", cur=current, tot=total, pct=pct))
        self.update_idletasks()

    def _start(self):
        if self._running:
            return
        d = self.dir_var.get().strip()
        if not d or not os.path.isdir(d):
            messagebox.showerror(t("dlg_no_dir_title"), t("dlg_no_folder"))
            return

        if not self.dry_run_var.get():
            if not messagebox.askyesno(t("dlg_backup_title"), t("dlg_backup_msg")):
                return

        self._running = True
        self.start_btn.configure(state="disabled", text=t("running_btn"))
        self.workers_slider.configure(state="disabled")
        self.workers_lock_label.configure(text="🔒")
        self._clear_log()
        self.progress["value"] = 0

        # Log-Buffer: sammelt Messages und flusht alle 150ms
        # verhindert GUI-Freeze durch 8000+ einzelne self.after() Aufrufe
        log_buffer = []
        log_lock = threading.Lock()

        def buffered_log(msg: str):
            with log_lock:
                log_buffer.append(msg)

        def flush_log():
            with log_lock:
                if log_buffer:
                    self._log("\n".join(log_buffer))
                    log_buffer.clear()
            if self._running:
                self.after(150, flush_log)

        # Progress-Updates auch throttlen (max 20/s)
        last_progress = [0.0]
        def throttled_progress(c, total_count):
            now = time.monotonic()
            if now - last_progress[0] >= 0.05 or c == total_count:
                last_progress[0] = now
                self.after(0, self._progress, c, total_count)

        self.after(150, flush_log)

        def worker():
            try:
                process_takeout(
                    d,
                    log_fn=buffered_log,
                    progress_fn=throttled_progress,
                    dry_run=self.dry_run_var.get(),
                    num_workers=self.workers_var.get(),
                )
            finally:
                # Letzten Buffer-Inhalt flushen
                with log_lock:
                    if log_buffer:
                        self.after(0, self._log, "\n".join(log_buffer))
                        log_buffer.clear()
                self.after(0, self._done)

        threading.Thread(target=worker, daemon=True).start()

    def _done(self):
        self._running = False
        self.start_btn.configure(state="normal", text=t("start_btn"))
        self.workers_slider.configure(state="normal")
        self.workers_lock_label.configure(text="")
        self.progress_label.configure(text=t("done_label"))


# ─── Einstiegspunkt ────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = App()
    app.mainloop()
