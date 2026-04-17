# SKILL: Subir script Python de análisis a GitHub para deploy en Fury

Guía paso a paso para replicar el proceso de empaquetar un script Python
que genera un reporte HTML y subirlo a GitHub listo para conectar con Fury (MeLi).

---

## Estructura de archivos necesaria

Todo script de análisis listo para Fury necesita estos archivos en el repo:

```
mi-repo/
├── mi_script.py          # Script principal de análisis
├── mi_output.html        # HTML de ejemplo (output del script)
├── Mercado_Ads.webp      # Logo de Mercado Ads (si el HTML lo usa)
├── requirements.txt      # Dependencias Python
├── .gitignore            # Archivos a ignorar
└── README.md             # Descripción del proyecto
```

---

## Paso 1 — Crear el repositorio en GitHub

1. Ir a https://github.com/new
2. Nombre del repo: nombre descriptivo en kebab-case (ej: `analisis-lanzamientos`)
3. Descripción corta de lo que hace el script
4. Visibility: **Private** si tiene datos reales de advertisers
5. Tildar **"Add a README file"**
6. Click en **"Create repository"**

---

## Paso 2 — Subir el script Python (.py)

El script Python normalmente es largo y no se puede tipear — se inserta via JavaScript en el editor de GitHub.

**Método — Editor web de GitHub:**
1. Ir a `https://github.com/[usuario]/[repo]/new/main`
2. En el campo "Name your file..." escribir el nombre: `mi_script.py`
3. Abrir la consola del browser (F12 → Console)
4. Usar el acceso al editor CodeMirror 6:
```javascript
const view = document.querySelector(".cm-content").cmTile.view;
view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: CONTENIDO } });
```
5. Donde `CONTENIDO` es el código Python como string (usar array de líneas con `.join("\n")` para evitar conflictos con backticks)
6. Click en **"Commit changes..."** → mensaje descriptivo → **"Commit changes"**

**Advertencia sobre strings en JS:**
- El código Python puede tener backticks (`) que rompen los template literals de JS
- Solución: construir el contenido con `["linea1", "linea2"].join("\n")` en vez de template literals
- Para f-strings de Python con `${}` usar strings normales con concatenación

---

## Paso 3 — Subir el HTML

El HTML generado por el script puede ser muy largo para el chat.
Se sube directo desde el filesystem:

1. Ir a `https://github.com/[usuario]/[repo]/upload/main`
2. Hacer click en **"choose your files"** o arrastrar el archivo
3. Esperar que aparezca el nombre del archivo abajo del área
4. Scrollear, poner mensaje de commit: `Add [nombre].html template`
5. Click en **"Commit changes"**

---

## Paso 4 — Subir el logo Mercado_Ads.webp

El logo es un archivo binario (.webp), no se puede crear desde el editor de texto.
Se sube igual que el HTML:

1. Ir a `https://github.com/[usuario]/[repo]/upload/main`
2. Usar `upload_image` con el ref del input de archivo oculto
3. Filename: `Mercado_Ads.webp`
4. Mensaje de commit: `Add Mercado_Ads.webp logo`

El script lo referencia así (no cambiar el nombre):
```python
_LOGO_PATH = os.path.join(os.path.dirname(__file__), "Mercado_Ads.webp")
```

---

## Paso 5 — Crear requirements.txt

Dependencias estándar para scripts de análisis con BigQuery + GCS:

```
pandas>=2.0.0
numpy>=1.24.0
google-cloud-bigquery>=3.11.0
google-cloud-bigquery-storage>=2.22.0
google-cloud-storage>=2.10.0
pyarrow>=12.0.0
db-dtypes>=1.1.1
```

Crear via editor web de GitHub:
1. Ir a `https://github.com/[usuario]/[repo]/new/main`
2. Nombre: `requirements.txt`
3. Pegar el contenido en el editor
4. Commit directo a main

---

## Paso 6 — Conectar a Fury

Una vez que el repo tiene todos los archivos, en Fury:

1. Responder las 3 preguntas iniciales de Fury:
   - **¿Tenés acceso a GitHub MeLi?** → Indicar si es repo personal o de la org
   - **¿Ya tenés repositorio?** → Sí, dar la URL completa
   - **¿Podés pasar el script?** → Dar el link al .py en el repo

2. Fury va a pedir crear/configurar:
   - La **app** en Fury (conectada al repo)
   - El **cron job** con la frecuencia deseada
   - Las **variables de entorno** (credenciales GCS, BQ project, etc.)
   - El **bucket de GCS** donde se sube el HTML generado

---

## Notas importantes

- **Repo público vs privado:** Si el HTML tiene datos reales de advertisers, hacerlo privado antes de subir el HTML
- **meli_bq:** El módulo `meli_bq` (usado para conectar a BigQuery) es interno de MeLi y está disponible en el entorno de Fury — no va en requirements.txt
- **FROM_CSV flag:** El script acepta `--from-csv` para correr sin consultar BQ, usando CSVs locales. Útil para desarrollo/debug local
- **Logo embebido:** El logo se convierte a base64 y se embebe en el HTML, así el HTML es self-contained (no depende de archivos externos cuando se sube a GCS)

---

## Checklist rápido

- [ ] Repo creado en GitHub
- [ ] `mi_script.py` subido
- [ ] `mi_output.html` subido
- [ ] `Mercado_Ads.webp` subido
- [ ] `requirements.txt` creado
- [ ] `.gitignore` presente
- [ ] App creada en Fury y conectada al repo
- [ ] Cron job configurado en Fury
- [ ] Variables de entorno / credenciales configuradas
- [ ] Bucket GCS definido para el output HTML
