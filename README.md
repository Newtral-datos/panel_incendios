<h1>Código del panel de incendios</h1>
Esta carpeta contiene los elementos necesarios para actualizar el panel de incendios y sus gráficos.
<h2>Objetivo del código</h2>
Actualizar con un solo script el panel de incendios y sus gráficos.
<h2>Funcionamiento</h2>
<p>En primer lugar descarga los datos más recientes de Copernicus relativos a incendios, los limpia y los transforma a formatos manejables. Después hace lo mismo con los de la NASA, que están centrados en los focos activos en las últimas 24h y 48h.</p>
<p>Los datos de la NASA se comparan con el polígono de España para aislar los datos que nos interesan. Después, todos estos datos, tratados y adaptados para nuestros gráficos, se suben a un Google Sheet, del cual bebe Flourish. Para subirlo es necesario tener el JSON con las credenciales en el directorio.</p>
<p></p>El ID del Google Sheet utilizado es: 1VVcr5c8TI_nPmx5KDcMmn-fAhRKU7BwlKc_rtDC-ycA</p>
<h2>Contenido</h2>
<b>Archivos</b>
<ul>
  <li>PANEL_INCENDIOS.py: es el <b>script principal</b>. Para usarlo hay que cambiar las rutas y adaptarlas a las tuyas. Nada más. Haz Ctrl+F para buscar "TU_RUTA_AQUI" y pon ahí tu ruta.</li>
  <li>poligono_españa.geojson: archivo <b>muy importante</b>. Sirve para hacer un análisis espacial con los datos de la NASA y ver los focos activos que hay en España.</li>
  <li>check.py: script sin importancia usado para sacar un par de estadísticas.</li>
  <li>recuento_años.xlsx y hectareas_quemadas_2025.xlsx: archivos generados en el script cuando no era automático. Almacenan estadísticas cada vez que se ejecuta el script principal.</li>
</ul>
<b>Carpetas</b>
<ul>
  <li>años_miteco: contiene los datos de Miteco de incendios desde 1968. Válidos hasta 2015 ya que los años siguientes son datos provisionales y no son consistentes. Incluye un .xlsx con los datos completos y tratados y un .py con el tratamiento.</li>
  <li>copernicus_datos_brutos: carpeta donde se descargan los datos brutos de Copernicus descargados con el script. No tocar.</li>
  <li>datos_limpios: carpeta donde se almacenan los resultados de Copernicus. No tocar.</li>
  <li>resultado: carpeta donde se almacenan segregados los datos de la NASA. No tocar.</li>
</ul>
