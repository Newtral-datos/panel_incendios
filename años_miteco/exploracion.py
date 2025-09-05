import os
import glob
import pandas as pd
from datetime import datetime
from datetime import datetime as _dt
from zoneinfo import ZoneInfo

CARPETA = r"C:/Users/miguel.ros/Desktop/Temas/Incendios/PANEL/años_miteco"
rutas = sorted(glob.glob(os.path.join(CARPETA, "*.xlsx")))

MANTER_COLUMNAS = [
    "Campania", "Comunidad", "Provincia", "Municipio",
    "Detectado", "SuperficieTotalForestal", "SuperficieTotalForestal_num"
]

def cargar_y_limpia(ruta_excel: str) -> pd.DataFrame:
    df = pd.read_excel(ruta_excel)
    serie_limpia = (
        df["SuperficieTotalForestal"]
        .astype(str)
        .str.replace(r"[^0-9.,]", "", regex=True)
        .str.replace(",", ".", regex=False)
    )
    df["SuperficieTotalForestal_num"] = pd.to_numeric(serie_limpia, errors="coerce")
    for c in MANTER_COLUMNAS:
        if c not in df.columns:
            df[c] = pd.NA
    df = df[MANTER_COLUMNAS].copy()
    df["archivo_origen"] = os.path.basename(ruta_excel)
    return df

print("Leyendo..:")
lista_df = [cargar_y_limpia(r) for r in rutas]
df_final = pd.concat(lista_df, ignore_index=True)

print("Limpiando...")
def metatesis(texto):
    if "," in texto:
        partes = texto.split(", ")
        return f"{partes[1]} {partes[0]}"
    return texto

df_final["Comunidad"] = df_final["Comunidad"].apply(metatesis)
df_final["Provincia"] = df_final["Provincia"].apply(metatesis)
df_final["Municipio"] = df_final["Municipio"].apply(metatesis)

df_final["Comunidad"] = df_final["Comunidad"].str.title()
df_final["Provincia"] = df_final["Provincia"].str.title()
df_final["Municipio"] = df_final["Municipio"].str.title()

df_final["Detectado"] = pd.to_datetime(df_final["Detectado"]).dt.strftime("%d/%m/%Y")
df_final["Superficie_TXT"] = df_final["SuperficieTotalForestal_num"].apply(lambda x: f"{x:,.0f}".replace(",", "X").replace(".", ",").replace("X", "."))

df_final["Comunidad"] = df_final["Comunidad"].replace("Castilla Y Leon", "Castilla Y León")
df_final["Comunidad"] = df_final["Comunidad"].replace("C. Valenciana", "Comunidad Valenciana")
df_final["Comunidad"] = df_final["Comunidad"].replace("Andalucia", "Andalucía")
df_final["Comunidad"] = df_final["Comunidad"].replace("Aragon", "Aragón")
df_final["Provincia"] = df_final["Provincia"].replace("Leon", "León")
df_final["Provincia"] = df_final["Provincia"].replace("Avila", "Ávila")
df_final["Provincia"] = df_final["Provincia"].replace("Caceres", "Cáceres")
df_final["Provincia"] = df_final["Provincia"].replace("Jaen", "Jaén")

df_final.to_excel("C:/Users/miguel.ros/Desktop/prueba.xlsx")

tabla = (
    df_final["Campania"]
    .value_counts()
    .reset_index()
    .rename(columns={"index": "Campania", "Campania": "año"})
)

df_final = df_final[df_final["Campania"] <= 2016]

df_final.drop(columns=[
    "SuperficieTotalForestal",
    "archivo_origen",
    "Comunidad"
], inplace=True)

df_final = df_final.rename(columns={
    "Campania": "AÑO",
    "Provincia": "PROVINCIA",
    "Municipio": "MUNICIPIO",
    "Detectado": "FECHA",
    "SuperficieTotalForestal_num": "AREA_HA",
    "Superficie_TXT": "AREA_HA_TXT"
})

df_final["FUENTE"] = "Miteco"

df_final["FIREDATE"] = pd.to_datetime(df_final["FECHA"], format="%d/%m/%Y")
df_final["FIREDATE"] = df_final["FIREDATE"].dt.strftime("%Y-%m-%d %H:%M:%S")

# ----- UNIR CON COPERNICUS.
fecha_copernicus = datetime.now(ZoneInfo("Europe/Madrid")).strftime("%d_%m_%Y")
directorio_copernicus = "C:/Users/miguel.ros/Desktop/Temas/Incendios/PANEL/"

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

# ----- UNIR DF.
print("Uniendo y exportando...")
datos = pd.concat([df_final, copernicus], ignore_index=True)

datos["MUNICIPIO"] = datos["MUNICIPIO"].replace("Indeterminado", "Ubicación indeterminada")
datos["MUNICIPIO"] = datos["MUNICIPIO"].replace("Otra Provincia", "Ubicación indeterminada")

datos.sort_values(by="AREA_HA", ascending=False) \
     .head(1000) \
     .to_excel(f"{directorio_copernicus}peores_incendios.xlsx", index=False)
