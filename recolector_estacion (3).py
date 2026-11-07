# -*- coding: utf-8 -*-
"""
Estación Anemométrica Loyola - Recolector local (versión de bajo consumo)
=========================================================================
Corre en un equipo dentro de la MISMA RED LOCAL que el datalogger. Funciona así:

    1. Lee el datalogger (histórico al arrancar; luego, cada minuto, solo lo nuevo).
       Esto es tráfico local: no cuesta nada en la nube.
    2. Guarda todo en una base SQLite local (respaldo): si se cae internet, no se pierde nada.
    3. Reenvía lo pendiente a Vercel EN LOTES cada `intervalo_envio_min` minutos (15 por defecto),
       en una sola conexión reutilizada y con reintentos espaciados si falla.

Por qué en lotes: el plan gratuito de Vercel limita las invocaciones (1 millón/mes) y la base
Neon gratuita limita las horas de cómputo (100 CU-horas/mes) y solo "duerme" si pasan más de
5 minutos sin peticiones. Con un envío cada 15 minutos la base duerme entre envíos y el
consumo queda muy por debajo de los límites. La "última lectura" de la página se obtiene del
registro más reciente de cada lote, así que ya no hay un envío "en vivo" aparte.

NO pongas intervalo_envio_min = 1: mantendría la base despierta las 24 h.

Requisitos:  pip install requests
Ejecutar:    python recolector_estacion.py
"""

import os
import sys
import json
import math
import time
import sqlite3
import logging
import threading
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET
import requests

# ============================ CONFIGURACIÓN ============================
def carpeta_base():
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

BASE_DIR = carpeta_base()
CONFIG_PATH = os.path.join(BASE_DIR, "config_recolector.json")
DB_PATH = os.path.join(BASE_DIR, "viento_local.db")
LOG_FILE = os.path.join(BASE_DIR, "recolector.log")

CONFIG_POR_DEFECTO = {
    "datalogger_url": "http://10.122.120.12/tables.html",
    "remoto_url": "https://ealoyola.vercel.app",     # sin barra final
    "remoto_token": "tesismeteorologicaestipl2026",                # igual a INGEST_TOKEN en Vercel
    # Cada cuántos minutos se envía un lote a la nube (mínimo recomendado: 10)
    "intervalo_envio_min": 15,
}

def cargar_config():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                datos = json.load(f)
            cfg = dict(CONFIG_POR_DEFECTO)
            cfg.update({k: v for k, v in datos.items() if k in CONFIG_POR_DEFECTO})
            return cfg
        except (OSError, ValueError, json.JSONDecodeError):
            pass
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(CONFIG_POR_DEFECTO, f, ensure_ascii=False, indent=2)
    print(f"Se creó {CONFIG_PATH} con valores de ejemplo.")
    print("Ábrelo, coloca la IP del datalogger, el enlace de Vercel y el token, y vuelve a ejecutar.")
    sys.exit(0)

config = cargar_config()

if "CAMBIAR" in config.get("remoto_url", "") or "CAMBIAR" in config.get("remoto_token", ""):
    print(f"Faltan datos por configurar en {CONFIG_PATH} (remoto_url / remoto_token).")
    sys.exit(0)

def _base_datalogger():
    return config["datalogger_url"].rstrip("/")

def url_backfill(p1):
    return f"{_base_datalogger()}?command=DataQuery&mode=backfill&format=xml&uri=dl:Datos&p1={p1}"

def url_since_record(p1, p3):
    return (f"{_base_datalogger()}?command=DataQuery&mode=since-record"
            f"&format=xml&uri=dl:Datos&p1={p1}&p3={p3}")

def url_ingesta_historico():
    return f"{config['remoto_url'].rstrip('/')}/api/ingest/historico"

# Una sola sesión HTTPS reutilizada (evita repetir el handshake TLS en cada envío)
sesion = requests.Session()
sesion.headers.update({"Content-Type": "application/json",
                       "Authorization": f"Bearer {config['remoto_token']}"})

INTERVALO_ENVIO_SEG = max(1, int(config.get("intervalo_envio_min", 15))) * 60
SEGUNDOS_HISTORICO = 94608000      # ~3 años hacia atrás
REFRESCO_HISTORICO_SEG = 60        # lectura local del datalogger, cada minuto
MAX_INCREMENTAL_SEGUNDOS = 259200  # la lectura de cada minuto pide como máximo los últimos 3 días
CARGA_PAGINA = 5000
LOTE_ENVIO = 2000                  # registros por petición (máximo que acepta el servidor)
PAUSA_ENTRE_LOTES_SEG = 0.5
ESPERA_CARGA_INICIAL_SEG = 20      # mientras se baja el histórico, se envía más seguido
DESFASE_SEG = 10                   # los envíos se alinean al reloj + 10 s

logging.basicConfig(filename=LOG_FILE, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logging.getLogger().addHandler(logging.StreamHandler())

# ============================ BASE DE DATOS (búfer local) ============================
def crear_tabla():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Datos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Fecha TEXT, Record INTEGER,
            Velviento_S_WVT REAL, Direccion_D1_WVT REAL, Direccion_SD1_WVT REAL,
            Velviento_Max REAL, Velviento_Std REAL,
            enviado INTEGER NOT NULL DEFAULT 0
        )
    """)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_datos_fecha ON Datos(Fecha)")
    cur.execute("CREATE INDEX IF NOT EXISTS ix_datos_enviado ON Datos(enviado)")
    con.commit()
    con.close()

crear_tabla()

# ================== LECTURA DEL DATALOGGER (backfill XML, streaming) ==================
estado_hist = {"cargando": False, "procesados": 0, "nuevos": 0, "error": None}
hist_lock = threading.Lock()

def _local(tag):
    return tag.split('}')[-1]

def minuto_exacto(txt):
    return str(txt).strip().replace('T', ' ')[:16] + ":00"

def registro_desde_elemento(rec):
    """<r time=".." no=".."> con <v1>..<v5> -> tupla para SQLite, o None si falta algo o hay NAN."""
    fecha_txt = rec.get('time')
    if not fecha_txt:
        return None
    valores = {}
    for hijo in rec:
        tag = _local(hijo.tag)
        if tag in ('v1', 'v2', 'v3', 'v4', 'v5') and hijo.text:
            try:
                valores[tag] = float(hijo.text)
            except ValueError:
                return None
    if len(valores) < 5 or any(math.isnan(x) for x in valores.values()):
        return None
    try:
        no = int(float(rec.get('no', 0)))
    except ValueError:
        no = 0
    return (minuto_exacto(fecha_txt), no,
            valores['v1'], valores['v2'], valores['v3'], valores['v4'], valores['v5'])

# El registro nuevo gana y queda enviado=0 si cambió respecto a lo guardado.
SQL_INSERT = """INSERT INTO Datos (Fecha, Record, Velviento_S_WVT, Direccion_D1_WVT,
                    Direccion_SD1_WVT, Velviento_Max, Velviento_Std, enviado)
                VALUES (?,?,?,?,?,?,?,0)
                ON CONFLICT(Fecha) DO UPDATE SET
                    Record = excluded.Record,
                    Velviento_S_WVT = excluded.Velviento_S_WVT,
                    Direccion_D1_WVT = excluded.Direccion_D1_WVT,
                    Direccion_SD1_WVT = excluded.Direccion_SD1_WVT,
                    Velviento_Max = excluded.Velviento_Max,
                    Velviento_Std = excluded.Velviento_Std,
                    enviado = 0
                WHERE Datos.Record IS NOT excluded.Record
                   OR Datos.Velviento_S_WVT IS NOT excluded.Velviento_S_WVT
                   OR Datos.Direccion_D1_WVT IS NOT excluded.Direccion_D1_WVT
                   OR Datos.Direccion_SD1_WVT IS NOT excluded.Direccion_SD1_WVT
                   OR Datos.Velviento_Max IS NOT excluded.Velviento_Max
                   OR Datos.Velviento_Std IS NOT excluded.Velviento_Std"""

def registros_desde_xml_stream(fuente):
    pila = []
    for evento, el in ET.iterparse(fuente, events=("start", "end")):
        if evento == "start":
            pila.append(el)
            continue
        pila.pop()
        if _local(el.tag) != 'r':
            continue
        reg = registro_desde_elemento(el)
        el.clear()
        if pila:
            pila[-1].remove(el)
        if reg is not None:
            yield reg

def completar_por_registro(con, desde_no):
    """Continúa por número de registro si el backfill llegó truncado."""
    siguiente = desde_no
    for _ in range(5000):
        try:
            with requests.get(url_since_record(siguiente, CARGA_PAGINA),
                              stream=True, timeout=180) as r:
                if r.status_code != 200:
                    break
                r.raw.decode_content = True
                regs = list(registros_desde_xml_stream(r.raw))
        except Exception as e:
            logging.error(f"Completar histórico: {e}")
            break
        if not regs:
            break
        con.executemany(SQL_INSERT, regs)
        con.commit()
        estado_hist["procesados"] += len(regs)
        siguiente = regs[-1][1] + 1
        if len(regs) < CARGA_PAGINA:
            break

def cargar_historico(incremental=False):
    """Descarga del datalogger a la base local. incremental=True pide solo lo reciente."""
    if not hist_lock.acquire(blocking=False):
        return
    estado_hist.update(cargando=not incremental, procesados=0, nuevos=0, error=None)
    p1 = SEGUNDOS_HISTORICO
    con = None
    try:
        con = sqlite3.connect(DB_PATH)
        if incremental:
            ultima = con.execute("SELECT MAX(Fecha) FROM Datos").fetchone()[0]
            if ultima:
                try:
                    dt = datetime.strptime(ultima[:19], "%Y-%m-%d %H:%M:%S")
                    p1 = min(MAX_INCREMENTAL_SEGUNDOS,
                             int((datetime.now() - dt).total_seconds()) + 180)
                except ValueError:
                    pass
        if not incremental:
            logging.info(f"Carga completa del histórico local, p1={p1}")

        with requests.get(url_backfill(p1), stream=True, timeout=(15, 600)) as r:
            if r.status_code != 200:
                raise RuntimeError(f"el datalogger respondió HTTP {r.status_code}")
            r.raw.decode_content = True
            lote = []
            ultimo_no, ultima_fecha = None, None
            cambios0 = con.total_changes
            for reg in registros_desde_xml_stream(r.raw):
                lote.append(reg)
                ultima_fecha, ultimo_no = reg[0], reg[1]
                if len(lote) >= 5000:
                    con.executemany(SQL_INSERT, lote)
                    con.commit()
                    estado_hist["procesados"] += len(lote)
                    lote.clear()
            if lote:
                con.executemany(SQL_INSERT, lote)
                con.commit()
                estado_hist["procesados"] += len(lote)
            if not incremental:
                logging.info(f"Backfill XML: último registro leído no={ultimo_no} fecha={ultima_fecha}")
            if ultimo_no is not None and ultima_fecha < (
                    datetime.now() - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S"):
                completar_por_registro(con, ultimo_no + 1)
            estado_hist["nuevos"] = con.total_changes - cambios0
        if not incremental or estado_hist["nuevos"] > 0:
            logging.info(f"Histórico local {'incremental' if incremental else 'completo'}: "
                         f"{estado_hist['procesados']} recibidos, {estado_hist['nuevos']} nuevos")
    except Exception as e:
        estado_hist["error"] = str(e)
        logging.error(f"Error leyendo el datalogger: {e}")
    finally:
        if con:
            con.close()
        estado_hist["cargando"] = False
        hist_lock.release()

def ciclo_historico(stop_event):
    cargar_historico(incremental=False)
    while not stop_event.wait(REFRESCO_HISTORICO_SEG - (time.time() % REFRESCO_HISTORICO_SEG) + 2):
        cargar_historico(incremental=True)

# ================== ENVÍO EN LOTES HACIA VERCEL ==================
COLUMNAS_JSON = ("Fecha", "Record", "Velviento_S_WVT", "Direccion_D1_WVT",
                 "Direccion_SD1_WVT", "Velviento_Max", "Velviento_Std")

def enviar_lote_historico(con):
    """Envía hasta LOTE_ENVIO filas pendientes. Devuelve cuántas se enviaron, 0 si no hay
    pendientes, o -1 si hubo un error (las filas quedan pendientes para el próximo intento)."""
    filas = con.execute(f"""SELECT id, {', '.join(COLUMNAS_JSON)} FROM Datos
                            WHERE enviado = 0 ORDER BY Fecha ASC LIMIT ?""",
                        (LOTE_ENVIO,)).fetchall()
    if not filas:
        return 0
    ids = [f[0] for f in filas]
    registros = [dict(zip(COLUMNAS_JSON, f[1:])) for f in filas]
    try:
        r = sesion.post(url_ingesta_historico(), json={"registros": registros}, timeout=60)
    except requests.RequestException as e:
        logging.warning(f"Sin conexión con el servidor remoto: {e}")
        return -1
    if r.status_code // 100 != 2:
        logging.warning(f"El servidor remoto rechazó el lote: HTTP {r.status_code} - {r.text[:200]}")
        return -1
    con.execute(f"UPDATE Datos SET enviado = 1 WHERE id IN ({','.join('?' * len(ids))})", ids)
    con.commit()
    return len(ids)

def ciclo_envio(stop_event):
    """Cada INTERVALO_ENVIO_SEG (alineado al reloj) vacía los pendientes en lotes.
    Si falla, reintenta con espera creciente (30 s, 60 s, ... hasta el intervalo normal)."""
    espera_error = 0
    while not stop_event.is_set():
        fallo = False
        con = sqlite3.connect(DB_PATH)
        try:
            while not stop_event.is_set():
                n = enviar_lote_historico(con)
                if n < 0:
                    fallo = True
                    break
                if n == 0:
                    break
                logging.info(f"Enviados {n} registros al servidor remoto")
                if stop_event.wait(PAUSA_ENTRE_LOTES_SEG):
                    break
        finally:
            con.close()

        if fallo:
            espera_error = min(INTERVALO_ENVIO_SEG, max(30, espera_error * 2))
            espera = espera_error
        else:
            espera_error = 0
            if estado_hist["cargando"]:      # bajando el histórico: seguir enviando sin esperar
                espera = ESPERA_CARGA_INICIAL_SEG
            else:
                espera = INTERVALO_ENVIO_SEG - ((time.time() - DESFASE_SEG) % INTERVALO_ENVIO_SEG)
        stop_event.wait(espera)

# ============================ ARRANQUE ============================
if __name__ == "__main__":
    logging.info("Recolector iniciado (modo bajo consumo)")
    logging.info(f"Datalogger: {config['datalogger_url']}")
    logging.info(f"Servidor remoto: {config['remoto_url']} · envío cada {INTERVALO_ENVIO_SEG // 60} min")

    stop_event = threading.Event()
    hilos = [
        threading.Thread(target=ciclo_historico, args=(stop_event,), daemon=True),
        threading.Thread(target=ciclo_envio, args=(stop_event,), daemon=True),
    ]
    for h in hilos:
        h.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo el recolector...")
    finally:
        stop_event.set()
