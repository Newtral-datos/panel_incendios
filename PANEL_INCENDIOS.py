# ----- IMPORTAR LIBRERÍAS.
import os
import io
import glob
import zipfile
import shutil
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import geopandas as gpd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from pandas.api.types import is_datetime64_any_dtype, is_datetime64tz_dtype
from datetime import datetime as _dt

# ----- FUNCIONES AUXILIARES
def hora():
    return datetime.now(ZoneInfo("Europe/Madrid")).strftime("%H:%M:%S")

# ----- CONFIGURACIÓN GENERAL
# FECHAS.
fecha_copernicus = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%d_%m_%Y")
fecha_str = fecha_copernicus

# RUTAS BASE.
directorio = "/Users/miguel.ros/Desktop/GITHUB/repositorio_panel_incendios/"
ruta_datos_copernicus = "copernicus_datos_brutos/"
ruta_salida = "datos_limpios/"
prefijo_fecha_copernicus = "data_copernicus_"
prefijo_fecha_limpia_copernicus = "incendios_"
nombre_archivo_copernicus = "modis.ba.poly"

carpeta_fecha = os.path.join(directorio, ruta_datos_copernicus, f"{prefijo_fecha_copernicus}{fecha_copernicus}")
os.makedirs(carpeta_fecha, exist_ok=True)
os.makedirs(os.path.join(directorio, ruta_salida), exist_ok=True)

# FIRMS: URLs y rutas.
url_firms_24h = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Europe_24h.zip"
url_firms_48h = "https://firms.modaps.eosdis.nasa.gov/data/active_fire/suomi-npp-viirs-c2/shapes/zips/SUOMI_VIIRS_C2_Europe_48h.zip"
ruta_poligono_espana = Path("/Users/miguel.ros/Desktop/PANEL_INCENDIOS/poligono_españa.geojson")
capa_poligono_espana = None
carpeta_descargas  = Path(directorio) / "descargas"
carpeta_extraccion = Path(directorio) / "extraccion"
carpeta_resultado  = Path(directorio) / "resultado"
carpeta_resultado.mkdir(parents=True, exist_ok=True)
predicado_espacial = "within"
cabeceras_http = {"User-Agent": "Mozilla/5.0 (compatible; DescargaFIRMS/1.0)"}

# GOOGLE SHEETS.
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]
cred_path = "/Users/miguel.ros/Desktop/GITHUB/repositorio_panel_incendios/credenciales_google_sheet.json"
spreadsheet_id = "1VVcr5c8TI_nPmx5KDcMmn-fAhRKU7BwlKc_rtDC-ycA"

# ----- COPERNICUS
wfs_url = ("https://maps.effis.emergency.copernicus.eu/effis?service=WFS&request=getfeature&typename=ms:modis.ba.poly&version=1.1.0&outputformat=SHAPEZIP")

print(f"[{hora()}]Descargando ZIP de Copernicus…")
resp = requests.get(wfs_url, timeout=120)
resp.raise_for_status()

zip_path = os.path.join(carpeta_fecha, "copernicus.zip")
with open(zip_path, "wb") as f:
    f.write(resp.content)

print(f"[{hora()}]Extrayendo ZIP…")
with zipfile.ZipFile(zip_path) as z:
    z.extractall(carpeta_fecha)

# Localizar el .shp principal.
ruta_shp_esperada = os.path.join(carpeta_fecha, f"{nombre_archivo_copernicus}.shp")
if os.path.exists(ruta_shp_esperada):
    ruta_geodata_copernicus = ruta_shp_esperada
else:
    shps = glob.glob(os.path.join(carpeta_fecha, "*.shp"))
    if not shps:
        raise FileNotFoundError("No se encontró ningún .shp tras extraer el ZIP de Copernicus.")
    ruta_geodata_copernicus = shps[0]
    print(f"[{hora()}]Aviso: usando {os.path.basename(ruta_geodata_copernicus)}.")

print(f"[{hora()}]Leyendo archivo SHP…")
geodata_copernicus = gpd.read_file(ruta_geodata_copernicus)

# LIMPIEZA DE DATOS COPERNICUS.
print(f"[{hora()}]Limpiando datos (Copernicus)…")
    ## Filtrar solo España.
geodata_copernicus = geodata_copernicus[geodata_copernicus["COUNTRY"] == "ES"]

    ## Formatear fecha y año.
geodata_copernicus["FECHA_INCENDIO"] = pd.to_datetime(
    geodata_copernicus["FIREDATE"], format="mixed", errors="coerce"
).dt.strftime("%d/%m/%Y")
geodata_copernicus["AÑO"] = geodata_copernicus["FECHA_INCENDIO"].str[-4:]

    ## Hectáreas a números con separador de miles.
geodata_copernicus["HECTAREAS"] = pd.to_numeric(geodata_copernicus["AREA_HA"], errors="coerce").apply(
    lambda x: f"{x:,.0f}".replace(",", ".") if pd.notnull(x) else pd.NA
)

    ## Ajustar nombres (Provincia/Municipio).
def metatesis(texto):
    if isinstance(texto, str) and "," in texto:
        partes = texto.split(", ")
        if len(partes) == 2:
            return f"{partes[1]} {partes[0]}"
    return texto

geodata_copernicus["PROVINCE"] = geodata_copernicus["PROVINCE"].apply(metatesis)
geodata_copernicus["COMMUNE"] = geodata_copernicus["COMMUNE"].apply(metatesis)

# REPROYECTAR Y CENTROIDES.
print(f"[{hora()}]Reproyectando y calculando centroides (Copernicus)…")
geodata_copernicus = geodata_copernicus.to_crs(epsg=4326)
geodata_copernicus["CENTROIDES"] = geodata_copernicus.geometry.centroid
geodata_copernicus["LATITUD"] = geodata_copernicus["CENTROIDES"].y
geodata_copernicus["LONGITUD"] = geodata_copernicus["CENTROIDES"].x
geodata_copernicus["CENTROIDES"] = geodata_copernicus["CENTROIDES"].to_wkt()

    ## Quitar columnas no necesarias.
cols_drop = [
    "id", "LASTUPDATE", "COUNTRY", "BROADLEA", "CONIFER", "MIXED", "SCLEROPH",
    "TRANSIT", "OTHERNATLC", "AGRIAREAS", "ARTIFSURF", "OTHERLC", "PERCNA2K", "CLASS"
]
geodata_copernicus = geodata_copernicus.drop(columns=[c for c in cols_drop if c in geodata_copernicus.columns])

# EXPORTACIÓN LOCAL COPERNICUS.
print(f"[{hora()}]Exportando Copernicus…")
geodata_copernicus.to_file(
    f"{directorio}{ruta_salida}{prefijo_fecha_limpia_copernicus}{fecha_copernicus}.geojson",
    driver="GeoJSON"
)
data_copernicus = geodata_copernicus.drop(columns=["geometry"])
data_copernicus.to_excel(
    f"{directorio}{ruta_salida}{prefijo_fecha_limpia_copernicus}{fecha_copernicus}.xlsx",
    index=False
)

# Resumen por años.
datos2025 = geodata_copernicus["AÑO"].value_counts()
datos2025.to_excel(f"{directorio}recuento_años.xlsx", index=True)

# ----- FIRMS
# FUNCIONES AUXILIARES FIRMS.
def descargar_zip(url: str, ruta_zip: Path):
    ## Descarga un ZIP y lo guarda en disco.
    print(f"[{hora()}]Descargando ZIP FIRMS…")
    ruta_zip.parent.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, headers=cabeceras_http, timeout=60)
    r.raise_for_status()
    ruta_zip.write_bytes(r.content)

def extraer_zip(ruta_zip: Path, carpeta_destino: Path) -> list[Path]:
    ## Extrae un ZIP y devuelve lista de archivos.
    print(f"[{hora()}]Extrayendo {ruta_zip} …")
    carpeta_destino.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(ruta_zip, "r") as zf:
        zf.extractall(carpeta_destino)
        return [carpeta_destino / n for n in zf.namelist()]

def buscar_shp(archivos: list[Path]) -> Path | None:
    ## Busca archivo .shp en lista.
    for p in archivos:
        if p.suffix.lower() == ".shp":
            return p
    return None

def cargar_poligono_local(ruta: Path, capa: str | None = None) -> gpd.GeoDataFrame:
    ## Carga polígono de España (geojson).
    gdf = gpd.read_file(ruta, layer=capa) if capa else gpd.read_file(ruta)
    if len(gdf) > 1:
        gdf = gdf[["geometry"]].dissolve()
    return gdf.to_crs(epsg=4326) if gdf.crs else gdf.set_crs(epsg=4326)

def cargar_filtrar_firms(url: str, carpeta_descargas: Path,
                         carpeta_extraccion: Path, espana: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    ## Descarga, extrae y filtra datos FIRMS para España.
    nombre_zip = Path(url).name
    ruta_zip = carpeta_descargas / nombre_zip
    descargar_zip(url, ruta_zip)
    archivos = extraer_zip(ruta_zip, carpeta_extraccion / Path(nombre_zip).stem)
    shp = buscar_shp(archivos)
    gdf = gpd.read_file(shp).to_crs(epsg=4326)
    gdf_es = gpd.sjoin(gdf, espana, how="inner", predicate=predicado_espacial)
    return gdf_es.drop(columns=["index_right"]) if "index_right" in gdf_es.columns else gdf_es

def normalizar_columnas(gdf: gpd.GeoDataFrame, etiqueta_ventana: str) -> gpd.GeoDataFrame:
    ## Normaliza columnas principales de FIRMS.
    gdf = gdf.to_crs(epsg=4326)
    cols_lower = {c.lower(): c for c in gdf.columns}
    gdf["LATITUDE"] = gdf[cols_lower["latitude"]] if "latitude" in cols_lower else gdf.geometry.y
    gdf["LONGITUDE"] = gdf[cols_lower["longitude"]] if "longitude" in cols_lower else gdf.geometry.x
    gdf["ACQ_DATE"] = gdf[cols_lower["acq_date"]].astype(str) if "acq_date" in cols_lower else ""
    gdf["ventana"] = etiqueta_ventana
    return gdf[["geometry", "LATITUDE", "LONGITUDE", "ACQ_DATE", "ventana"]]

# PROCESO FIRMS.
print(f"[{hora()}]Iniciando proceso FIRMS…")
espana = cargar_poligono_local(ruta_poligono_espana, capa=capa_poligono_espana)

exportado_ok = False
try:
    gdf_24h = normalizar_columnas(cargar_filtrar_firms(url_firms_24h, carpeta_descargas, carpeta_extraccion, espana), "nuevo")
    gdf_48h = normalizar_columnas(cargar_filtrar_firms(url_firms_48h, carpeta_descargas, carpeta_extraccion, espana), "ultimos")
    gdf_combinado = pd.concat([gdf_24h, gdf_48h], ignore_index=True).sort_values(by="ventana", ascending=False)

    # Guardar geojson locales.
    gdf_24h.to_file(carpeta_resultado / f"nuevo_{fecha_str}.geojson", driver="GeoJSON", index=False)
    gdf_48h.to_file(carpeta_resultado / f"ultimos_{fecha_str}.geojson", driver="GeoJSON", index=False)
    gdf_combinado.to_file(carpeta_resultado / f"conjunto_{fecha_str}.geojson", driver="GeoJSON", index=False)

    exportado_ok = True
finally:
    ## Limpieza de carpetas temporales.
    if exportado_ok:
        if carpeta_descargas.exists():
            shutil.rmtree(carpeta_descargas, ignore_errors=False)
        if carpeta_extraccion.exists():
            shutil.rmtree(carpeta_extraccion, ignore_errors=False)

# CONVERTIR A EXCEL.
print(f"[{hora()}]Convirtiendo FIRMS a Excel…")
datos_conjuntos = gpd.read_file(str(carpeta_resultado / f"conjunto_{fecha_str}.geojson")).drop(columns=["geometry"])
xlsx_conj = carpeta_resultado / f"conjunto_{fecha_str}.xlsx"
datos_conjuntos.to_excel(xlsx_conj, index=False)

# Guardar también 24h y 48h.
gdf_24h.drop(columns=["geometry"]).to_excel(carpeta_resultado / f"nuevo_{fecha_str}.xlsx", index=False)
gdf_48h.drop(columns=["geometry"]).to_excel(carpeta_resultado / f"ultimos_{fecha_str}.xlsx", index=False)

print(f"{len(gdf_24h)} FOCOS ACTIVOS")

# Leer como texto (evitar Timestamps).
datos_conjuntos = pd.read_excel(xlsx_conj, dtype=str)
df_24h = pd.read_excel(carpeta_resultado / f"nuevo_{fecha_str}.xlsx", dtype=str)
df_48h = pd.read_excel(carpeta_resultado / f"ultimos_{fecha_str}.xlsx", dtype=str)

# ----- SUBIDA A GOOGLE SHEETS
import math, time, re, httplib2
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_httplib2 import AuthorizedHttp
from datetime import datetime as _dt

creds = Credentials.from_service_account_file(cred_path, scopes=scope)

# Timeout ampliado
_http = httplib2.Http(timeout=500)
_authed_http = AuthorizedHttp(creds, http=_http)
service = build("sheets", "v4", http=_authed_http, cache_discovery=False)

def _col_to_a1(col_idx_0based: int) -> str:
    s, x = "", col_idx_0based + 1
    while x:
        x, r = divmod(x - 1, 26)
        s = chr(65 + r) + s
    return s

def _parse_a1(cell: str):
    m = re.match(r"^([A-Za-z]+)(\d+)?$", cell)
    if not m:
        return "A", 1
    col, row = m.group(1).upper(), int(m.group(2) or 1)
    return col, row

def _exec_with_retries(req, tries=5, base_sleep=1.5):
    for i in range(tries):
        try:
            return req.execute(num_retries=5)
        except (TimeoutError, HttpError):
            if i == tries - 1:
                raise
            time.sleep(base_sleep * (2 ** i))

def subir_df_a_sheet(df: pd.DataFrame, rango_inicial: str, pestaña: str, chunk_rows: int = 2000):
    df = df.copy()
    for c in ["LATITUD", "LONGITUD", "LATITUDE", "LONGITUDE"]:
        if c in df.columns:
            df[c] = df[c].astype(str)

    for col in df.columns:
        if is_datetime64_any_dtype(df[col]) or is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    def _to_str_safe(x):
        if isinstance(x, (pd.Timestamp, _dt)):
            return x.strftime("%Y-%m-%d %H:%M:%S")
        return x
    df = df.applymap(_to_str_safe)
    df = df.where(pd.notnull(df), None)

    # --- Limpiar hoja
    print(f"[{hora()}]Limpiando hoja '{pestaña}' …")
    _exec_with_retries(
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=f"{pestaña}!A1:ZZ"
        )
    )

    # --- Preparar valores: cabecera + datos
    header = list(map(str, df.columns.tolist()))
    rows = [[("" if v is None else str(v)) for v in row] for row in df.to_numpy().tolist()]

    # --- Calcular rango inicial
    a1_cell = rango_inicial.replace(f"{pestaña}!", "")
    start_col_letters, start_row = _parse_a1(a1_cell)

    # --- Escribir cabecera
    header_range = f"{pestaña}!{start_col_letters}{start_row}"
    _exec_with_retries(
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=header_range,
            valueInputOption="RAW",
            body={"values": [header]},
        )
    )

    # --- Escribir datos en bloques
    if not rows:
        print(f"[{hora()}]No hay filas para subir en '{pestaña}'.")
        return

    start_data_row = start_row + 1
    total = len(rows)
    n_chunks = math.ceil(total / chunk_rows)
    print(f"[{hora()}]Subiendo datos a '{pestaña}' en {n_chunks} bloque(s) de hasta {chunk_rows} fila(s)…")

    for i in range(n_chunks):
        i0, i1 = i * chunk_rows, min((i + 1) * chunk_rows, total)
        block = rows[i0:i1]
        write_range = f"{pestaña}!{start_col_letters}{start_data_row + i0}"
        _exec_with_retries(
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=write_range,
                valueInputOption="RAW",
                body={"values": block},
            )
        )
        print(f"[{hora()}]  · Bloque {i+1}/{n_chunks} ({i1 - i0} filas) OK")

# ----- GRÁFICOS.
# Hectáreas quemadas.
carpeta_datos = "/Users/miguel.ros/Desktop/PANEL_INCENDIOS/"
excel = pd.read_excel(f"{carpeta_datos}datos_limpios/incendios_{fecha_copernicus}.xlsx")
geodata = gpd.read_file(f"{carpeta_datos}datos_limpios/incendios_{fecha_copernicus}.geojson")

geodata = geodata[geodata["AÑO"] == "2025"]
geodata["FIREDATE"] = pd.to_datetime(geodata["FIREDATE"])
geodata["AREA_HA"] = pd.to_numeric(geodata["AREA_HA"], errors="coerce")
hectareas = geodata.groupby(geodata["FIREDATE"].dt.date)["AREA_HA"].sum().reset_index()
hectareas["FIREDATE"] = pd.to_datetime(hectareas["FIREDATE"])
hectareas["fecha"] = hectareas["FIREDATE"].dt.strftime("%d/%m/%Y")
hectareas["AREA_HA_TXT"] = hectareas["AREA_HA"].apply(lambda x: f"{int(round(x)):,}".replace(",", "X").replace(".", ",").replace("X", "."))
hectareas.to_excel(f"{carpeta_datos}/hectareas_quemadas_2025.xlsx")

# Unir Miteco con Copernicus.
fecha_copernicus = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%d_%m_%Y")
directorio_copernicus = "/Users/miguel.ros/Desktop/PANEL_INCENDIOS/"

df_final = pd.read_excel(f"{directorio_copernicus}/años_miteco/miteco_completo.xlsx")

copernicus = pd.read_excel(f"{directorio_copernicus}datos_limpios/incendios_{fecha_copernicus}.xlsx")

copernicus.drop(columns=[
    "CENTROIDES",
    "LATITUD",
    "LONGITUD",
    "HECTAREAS"
], inplace=True)

copernicus = copernicus.rename(columns={
    "PROVINCE": "PROVINCIA",
    "COMMUNE": "MUNICIPIO",
    "FECHA_INCENDIO": "FECHA",
})

copernicus["AREA_HA_TXT"] = copernicus["AREA_HA"].apply(lambda x: f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))
copernicus["FUENTE"] = "Copernicus"
copernicus = copernicus[copernicus["AÑO"] >= 2017]
# Unir DFs.
print("Uniendo y exportando...")
datos_peores_incendios = pd.concat([df_final, copernicus], ignore_index=True)

datos_peores_incendios["MUNICIPIO"] = datos_peores_incendios["MUNICIPIO"].replace("Indeterminado", "Ubicación indeterminada")
datos_peores_incendios["MUNICIPIO"] = datos_peores_incendios["MUNICIPIO"].replace("Otra Provincia", "Ubicación indeterminada")

datos_peores_incendios = pd.DataFrame(datos_peores_incendios)

datos_peores_incendios = datos_peores_incendios.sort_values(
    by="AREA_HA", ascending=False
).head(1000)

# ----- DATOS ESTADÍSTICOS.
# Focos activos.
activos = pd.read_excel(f"{carpeta_datos}resultado/nuevo_{fecha_copernicus}.xlsx")
print(f"Hay {len(activos)} focos activos.")
# Incendios en 2025.
incendios_2025 = pd.read_excel(f"{carpeta_datos}datos_limpios/incendios_{fecha_copernicus}.xlsx")
incendios_2025 = incendios_2025[incendios_2025["AÑO"] == 2025]
print(f"Incendios en 2025: {len(incendios_2025)}")
# Hectáreas.
excel = pd.read_excel(f"{carpeta_datos}/hectareas_quemadas_2025.xlsx")
hectareas_quemadas_2025 = excel["AREA_HA"].sum()
print(f"Hectáreas quemadas en 2025: {hectareas_quemadas_2025}")
# Fecha de actualización.
ahora = datetime.now()
fecha_actualizacion = ahora.strftime("%d/%m/%Y a las %H:%M")
print(f"Actualización: {fecha_actualizacion}")

datos_observable = {
    "focos_activos": [len(activos)],
    "incendios_2025": [len(incendios_2025)],
    "hectareas_2025": [hectareas_quemadas_2025],
    "actualizacion": [fecha_actualizacion]
}

datos_observable = pd.DataFrame(datos_observable)

for col in ["focos_activos", "incendios_2025", "hectareas_2025"]:
    datos_observable[col] = datos_observable[col].apply(lambda x: f"{int(x):,}".replace(",", "."))

# ----- SUBIDAS.
subir_df_a_sheet(datos_observable.copy(), "datos!A1", "datos")
subir_df_a_sheet(datos_conjuntos.copy(), "activos!A1", "activos")
subir_df_a_sheet(data_copernicus.copy(), "incendios!A1", "incendios")
subir_df_a_sheet(hectareas.copy(), "hectareas!A1", "hectareas")
subir_df_a_sheet(datos_peores_incendios.copy(), "peores_incendios!A1", "peores_incendios")

print(f"[{hora()}]✅ Todo ha salido a pedir de Milhouse.")


