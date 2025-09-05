import pandas as pd
import geopandas as gpd

directorio = "C:/Users/miguel.ros/Desktop/Temas/Incendios/PANEL/"

activos = pd.read_excel(f"{directorio}resultado/nuevo_20_08_2025.xlsx")
print(f"Hay {len(activos)} incendios activos en España en las últimas 24h.")

incendios = pd.read_excel(f"{directorio}datos_limpios/incendios_20_08_2025.xlsx")
incendios = incendios[incendios["AÑO"] == 2025]
print(f"Llevamos {len(incendios)} incendios en 2025.")

hectareas = incendios["AREA_HA"].sum()
print(f"Han ardido {hectareas} hectáreas en 2025.")


