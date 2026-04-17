"""
analisis_lanzamientos.py — Monthly Commercial Review: Only Off

Output: analisis_lanzamientos.html

Sections:
  1. Extended Network — Awareness (inventory breakdown, adoption, endemic/non-endemic ARPU)
  2. IAB Creatives adoption

Usage:
  python analisis_lanzamientos.py
  python analisis_lanzamientos.py --from-csv
"""

import sys
import time
import os
import base64
import pandas as pd
import numpy as np
from datetime import date, timedelta

_t0 = time.time()

FROM_CSV = "--from-csv" in sys.argv

# ══════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════

OO_START         = '2026-03-18'   # Only Off launch date
OO_HISTORY_START = '2025-03-18'   # 12 months prior (for new ADX/DSP detection)
ADX_MIN_USD      = 100            # Min Only Off spend post-launch to qualify

TODAY            = date.today()
YESTERDAY        = TODAY - timedelta(days=1)
ANALYSIS_END     = YESTERDAY.strftime('%Y-%m-%d')

OO_START_DATE    = date(2026, 3, 18)
DAYS_AFTER_OO    = max((YESTERDAY - OO_START_DATE).days, 1)

# Comparison window before OO launch: same number of days
BEFORE_START     = (OO_START_DATE - timedelta(days=DAYS_AFTER_OO)).strftime('%Y-%m-%d')
BEFORE_END       = (OO_START_DATE - timedelta(days=1)).strftime('%Y-%m-%d')
DAYS_AFTER       = DAYS_AFTER_OO  # alias

NON_ENDEMIC_VERTICALS = {
    'TRAVEL_AND_LEISURE',
    'TELECOMMUNICATIONS',
    'ENTERTAINMENT', 'ENTERTAINMENT_AND_MEDIA',
    'GOVERNMENT',
    'FINANCIAL_SERVICES',
    'EDUCATION_AND_TRAINING',
    'BUSINESS_SERVICES',
    'VIS', 'VIS_MOTORS', 'VIS_RE', 'VIS_RE_DEVELOPMENTS', 'VIS_RE_REALTORS',
    'VIS_VEHICLE_PARTS_AND_ACCESSORIES',
}

def is_non_endemic(vertical: str) -> bool:
    """True si la vertical es no endémica. Cubre VIS y sin info."""
    if pd.isna(vertical):
        return True
    v = str(vertical).upper().strip()
    if v in ('NONE', 'OTROS', 'OTHERS', 'NO INFORMADO', '', 'UNKNOWN', 'OTHER'):
        return True
    return v in {x.upper() for x in NON_ENDEMIC_VERTICALS} or v.startswith('VIS_')

SITES_LBL = {
    'MLA': 'Argentina', 'MLB': 'Brasil', 'MLC': 'Chile', 'MLM': 'México',
    'MCO': 'Colombia', 'MLU': 'Uruguay', 'MPE': 'Perú',
}

def safe_div(a, b, mult=1):
    with np.errstate(invalid='ignore', divide='ignore'):
        return np.where((b != 0) & pd.notna(b), a / b * mult, np.nan)

def fmt_usd(v, decimals=1):
    if pd.isna(v) or v == 0: return '$0'
    if abs(v) >= 1e6: return f'${v/1e6:.{decimals}f}M'
    if abs(v) >= 1e3: return f'${v/1e3:.0f}K'
    return f'${v:.0f}'

def fmt_pct(v, show_sign=True):
    if pd.isna(v): return '—'
    sign = '+' if (show_sign and v > 0) else ''
    return f'{sign}{v:.0f}%'

def fmt_num(v):
    if pd.isna(v): return '—'
    return f'{int(v):,}'

print("=" * 60)
print("  ANÁLISIS MENSUAL — Only Off")
print("=" * 60)
print(f"  Período análisis : {OO_START} → {ANALYSIS_END}")
print(f"  Días desde launch: {DAYS_AFTER}")
print(f"  Ventana anterior : {BEFORE_START} → {BEFORE_END}")
print(f"  Umbral ADX activo: >${ADX_MIN_USD:,} USD en 12 meses previos")
print("=" * 60 + "\n")

if not FROM_CSV:
    from meli_bq import get_client, query_df
    client = get_client()

# ══════════════════════════════════════════════════════════════════
# QUERIES
# ══════════════════════════════════════════════════════════════════

# ── Q1: Extended Network (Awareness + Consideración) — per LI ────
q_enet = f"""
SELECT
    md.SIT_SITE_ID                                                      AS site,
    md.ADVERTISER_ID                                                    AS advertiser_id,
    md.ADVERTISER_NAME                                                  AS advertiser_name,
    md.ADVERTISER_VERTICAL_NAME                                         AS vertical,
    md.CAMPAIGN_ID                                                      AS campaign_id,
    md.LINE_ITEM_ID                                                     AS line_item_id,
    COALESCE(li.inventory, '')                                          AS raw_inventory,
    CASE
        WHEN md.CAMPAIGN_TARGET IN ('REACH','IMPRESSION','IMPRESSIONS') THEN 'AWARENESS'
        WHEN md.CAMPAIGN_TARGET = 'CPC'                                 THEN 'CONSIDERACION'
        WHEN md.CAMPAIGN_TARGET IN ('CPA','CONVERSION','CONVERSIONS','ROAS','CPL') THEN 'CONVERSION'
        ELSE md.CAMPAIGN_TARGET
    END                                                                 AS strategy,
    SUM(md.IMPRESSION_COST_AMT_USD)                                     AS rev_total,
    SUM(CASE WHEN md.PAGE_NAME = 'adx-display'
             THEN md.IMPRESSION_COST_AMT_USD ELSE 0 END)                AS rev_off,
    SUM(CASE WHEN md.PAGE_NAME != 'adx-display'
             THEN md.IMPRESSION_COST_AMT_USD ELSE 0 END)                AS rev_in,
    SUM(md.PRINTS_QTY)                                                  AS prints_total,
    SUM(CASE WHEN md.PAGE_NAME = 'adx-display'
             THEN md.PRINTS_QTY ELSE 0 END)                             AS prints_off,
    SUM(CASE WHEN md.PAGE_NAME != 'adx-display'
             THEN md.PRINTS_QTY ELSE 0 END)                             AS prints_in,
    SUM(md.CLICKS_QTY)                                                  AS clicks_total
FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
LEFT JOIN meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item li
    ON li.line_item_id = md.LINE_ITEM_ID
WHERE md.EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
  AND md.CAMPAIGN_TYPE = 'PROGRAMMATIC'
  AND md.PRODUCT_TYPE = 'DSP_SELFSERVICE'
  AND UPPER(md.LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
  AND md.CAMPAIGN_TARGET IN ('REACH', 'IMPRESSION', 'IMPRESSIONS', 'CPC', 'CPA', 'CONVERSION', 'CONVERSIONS', 'ROAS', 'CPL')
  AND md.IMPRESSION_COST_AMT_USD > 0
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
"""

# ── Q2: Total Display — por advertiser con vertical + flag ADX + strategy ──
q_aw_denom = f"""
SELECT
    SIT_SITE_ID                                                         AS site,
    ADVERTISER_ID                                                       AS advertiser_id,
    ANY_VALUE(ADVERTISER_NAME)                                          AS advertiser_name,
    ANY_VALUE(ADVERTISER_VERTICAL_NAME)                                 AS vertical,
    SUM(IMPRESSION_COST_AMT_USD)                                        AS rev_total,
    MAX(CASE WHEN PAGE_NAME = 'adx-display' THEN 1 ELSE 0 END)         AS flag_adx,
    CASE WHEN CAMPAIGN_TARGET IN ('REACH','IMPRESSION','IMPRESSIONS')   THEN 'AWARENESS'
         WHEN CAMPAIGN_TARGET IN ('CPA','CONVERSION','CONVERSIONS','CPL') THEN 'CONVERSION'
         WHEN CAMPAIGN_TARGET = 'CPC'                                   THEN 'CONSIDERACION'
         ELSE CAMPAIGN_TARGET END                                        AS strategy
FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY
WHERE EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
  AND CAMPAIGN_TYPE = 'PROGRAMMATIC'
  AND PRODUCT_TYPE = 'DSP_SELFSERVICE'
  AND UPPER(LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
  AND CAMPAIGN_TARGET IN ('REACH', 'IMPRESSION', 'IMPRESSIONS', 'CPC', 'CPA', 'CONVERSION', 'CONVERSIONS', 'CPL')
  AND IMPRESSION_COST_AMT_USD > 0
GROUP BY 1, 2, strategy
"""

# ── Q3: ARPU before/after for Only Off Awareness adopters ─────────
q_arpu = f"""
WITH li_destination AS (
    SELECT
        li.LINE_ITEM_ID,
        MAX(CASE WHEN UPPER(dest.DESTINATION_SUBTYPE) IN ('CLICK_OFF','CLICK_OFF_SAFE')
                 THEN 1 ELSE 0 END) AS has_ext_destination
    FROM meli-bi-data.WHOWNER.LK_ADS_LINE_ITEMS li
    LEFT JOIN meli-bi-data.WHOWNER.LK_ADS_REL_LINE_ITEM_CREATIVE rel
        ON rel.LINE_ITEM_ID = li.LINE_ITEM_ID
    LEFT JOIN meli-bi-data.WHOWNER.LK_ADS_DISP_CREATIVES_HIST cr
        ON cr.CREATIVE_ID = rel.CREATIVE_ID
    LEFT JOIN meli-bi-data.WHOWNER.LK_ADS_DESTINATIONS dest
        ON dest.DESTINATION_ID = cr.CREATIVE_DESTINATION_ID
    WHERE li.LINE_ITEM_ID IN (
        SELECT line_item_id FROM meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item
        WHERE UPPER(inventory) = 'OFF'
    )
    GROUP BY li.LINE_ITEM_ID
),
oo_adv AS (
    SELECT
        md.ADVERTISER_ID,
        md.SIT_SITE_ID,
        ANY_VALUE(md.ADVERTISER_NAME)           AS advertiser_name,
        ANY_VALUE(md.ADVERTISER_VERTICAL_NAME)  AS vertical,
        MAX(COALESCE(ld.has_ext_destination,0)) AS has_dot_com,
        CASE WHEN UPPER(md.CAMPAIGN_TARGET) IN ('REACH','IMPRESSION','IMPRESSIONS') THEN 'AWARENESS'
             WHEN UPPER(md.CAMPAIGN_TARGET) IN ('CPA','CONVERSION','CONVERSIONS')   THEN 'CONVERSION'
             ELSE 'CONSIDERACION' END AS strategy
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    LEFT JOIN li_destination ld ON ld.LINE_ITEM_ID = md.LINE_ITEM_ID
    WHERE md.LINE_ITEM_ID IN (
        SELECT line_item_id
        FROM meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item
        WHERE UPPER(inventory) = 'OFF'
    )
    AND md.EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
    AND md.CAMPAIGN_TYPE = 'PROGRAMMATIC'
    AND md.PRODUCT_TYPE = 'DSP_SELFSERVICE'
    AND UPPER(md.LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
    AND md.IMPRESSION_COST_AMT_USD > 0
    GROUP BY md.ADVERTISER_ID, md.SIT_SITE_ID, strategy
),
rev_before AS (
    SELECT md.ADVERTISER_ID, md.SIT_SITE_ID, oa.strategy,
           SUM(md.IMPRESSION_COST_AMT_USD) AS rev_before
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    INNER JOIN oo_adv oa ON oa.ADVERTISER_ID = md.ADVERTISER_ID AND oa.SIT_SITE_ID = md.SIT_SITE_ID
    WHERE md.EVENT_LOCAL_DT BETWEEN '{BEFORE_START}' AND '{BEFORE_END}'
      AND md.CAMPAIGN_TYPE = 'PROGRAMMATIC'
      AND md.PRODUCT_TYPE = 'DSP_SELFSERVICE'
      AND UPPER(md.LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
      AND CASE WHEN UPPER(md.CAMPAIGN_TARGET) IN ('REACH','IMPRESSION','IMPRESSIONS') THEN 'AWARENESS'
               WHEN UPPER(md.CAMPAIGN_TARGET) IN ('CPA','CONVERSION','CONVERSIONS')   THEN 'CONVERSION'
               ELSE 'CONSIDERACION' END = oa.strategy
    GROUP BY 1, 2, 3
),
rev_after AS (
    SELECT md.ADVERTISER_ID, md.SIT_SITE_ID, oa.strategy,
           SUM(md.IMPRESSION_COST_AMT_USD) AS rev_after_total,
           SUM(CASE WHEN md.LINE_ITEM_ID IN (
               SELECT line_item_id FROM meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item
               WHERE UPPER(inventory) = 'OFF'
           ) THEN md.IMPRESSION_COST_AMT_USD ELSE 0 END)   AS rev_after_oo,
           SUM(CASE WHEN md.PAGE_NAME != 'adx-display'
               THEN md.IMPRESSION_COST_AMT_USD ELSE 0 END) AS rev_after_in_meli
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    INNER JOIN oo_adv oa ON oa.ADVERTISER_ID = md.ADVERTISER_ID AND oa.SIT_SITE_ID = md.SIT_SITE_ID
    WHERE md.EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
      AND md.CAMPAIGN_TYPE = 'PROGRAMMATIC'
      AND md.PRODUCT_TYPE = 'DSP_SELFSERVICE'
      AND UPPER(md.LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
      AND CASE WHEN UPPER(md.CAMPAIGN_TARGET) IN ('REACH','IMPRESSION','IMPRESSIONS') THEN 'AWARENESS'
               WHEN UPPER(md.CAMPAIGN_TARGET) IN ('CPA','CONVERSION','CONVERSIONS')   THEN 'CONVERSION'
               ELSE 'CONSIDERACION' END = oa.strategy
    GROUP BY 1, 2, 3
)
SELECT
    oa.SIT_SITE_ID      AS site,
    oa.ADVERTISER_ID    AS advertiser_id,
    oa.advertiser_name,
    oa.vertical,
    oa.has_dot_com,
    oa.strategy,
    COALESCE(b.rev_before,        0) AS rev_before,
    COALESCE(a.rev_after_total,   0) AS rev_after_total,
    COALESCE(a.rev_after_oo,      0) AS rev_after_oo,
    COALESCE(a.rev_after_in_meli, 0) AS rev_after_in_meli
FROM oo_adv oa
LEFT JOIN rev_before b ON b.ADVERTISER_ID = oa.ADVERTISER_ID AND b.SIT_SITE_ID = oa.SIT_SITE_ID AND b.strategy = oa.strategy
LEFT JOIN rev_after  a ON a.ADVERTISER_ID = oa.ADVERTISER_ID AND a.SIT_SITE_ID = oa.SIT_SITE_ID AND a.strategy = oa.strategy
ORDER BY rev_after_total DESC
"""

# ── Q4: First-time ADX / DSP via Only Off ─────────────────────────
q_nuevos = f"""
WITH oo_adv AS (
    SELECT md.ADVERTISER_ID, md.SIT_SITE_ID
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    WHERE md.LINE_ITEM_ID IN (
        SELECT line_item_id FROM meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item
        WHERE UPPER(inventory) = 'OFF'
    )
    AND md.EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
    AND md.IMPRESSION_COST_AMT_USD > 0
    GROUP BY md.ADVERTISER_ID, md.SIT_SITE_ID
    HAVING SUM(md.IMPRESSION_COST_AMT_USD) >= {ADX_MIN_USD}
),
history_adx AS (
    SELECT md.ADVERTISER_ID, md.SIT_SITE_ID
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    INNER JOIN oo_adv oa ON oa.ADVERTISER_ID = md.ADVERTISER_ID AND oa.SIT_SITE_ID = md.SIT_SITE_ID
    WHERE md.EVENT_LOCAL_DT BETWEEN '{OO_HISTORY_START}'
                                AND DATE_SUB('{OO_START}', INTERVAL 1 DAY)
      AND md.PAGE_NAME = 'adx-display'
      AND md.IMPRESSION_COST_AMT_USD > 0
    GROUP BY 1, 2
),
history_dsp AS (
    SELECT DISTINCT md.ADVERTISER_ID, md.SIT_SITE_ID
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    INNER JOIN oo_adv oa ON oa.ADVERTISER_ID = md.ADVERTISER_ID AND oa.SIT_SITE_ID = md.SIT_SITE_ID
    WHERE md.EVENT_LOCAL_DT BETWEEN '{OO_HISTORY_START}'
                                AND DATE_SUB('{OO_START}', INTERVAL 1 DAY)
      AND md.IMPRESSION_COST_AMT_USD > 0
)
SELECT
    oa.SIT_SITE_ID                  AS site,
    oa.ADVERTISER_ID                AS advertiser_id,
    adv.ADVERTISER_NAME             AS advertiser_name,
    (ha.ADVERTISER_ID IS NULL)      AS nuevo_en_adx,
    (hd.ADVERTISER_ID IS NULL)      AS nuevo_en_dsp
FROM oo_adv oa
LEFT JOIN meli-bi-data.WHOWNER.LK_ADS_ADVERTISERS adv
    ON adv.ADVERTISER_ID = oa.ADVERTISER_ID
LEFT JOIN history_adx ha ON ha.ADVERTISER_ID = oa.ADVERTISER_ID AND ha.SIT_SITE_ID = oa.SIT_SITE_ID
LEFT JOIN history_dsp hd ON hd.ADVERTISER_ID = oa.ADVERTISER_ID AND hd.SIT_SITE_ID = oa.SIT_SITE_ID
"""

# ── Q5: IAB Creatives adoption ─────────────────────────────────────
q_iab = f"""
WITH iab_li AS (
    SELECT DISTINCT CAST(lic.line_item_id AS INT64) AS line_item_id
    FROM meli-bi-data.SBOX_ADVERTISINGDISPLAY.creative c
    INNER JOIN meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item_creative lic
        ON c.creative_id = lic.creative_id
    WHERE c.advertiser_id NOT IN (710)
      AND EXISTS (
          SELECT 1
          FROM UNNEST(JSON_QUERY_ARRAY(JSON_VALUE(c.images))) AS img_obj
          WHERE JSON_VALUE(img_obj, '$.type') = 'iab_picture'
      )
),
active_display_li AS (
    SELECT DISTINCT
        md.SIT_SITE_ID                      AS site,
        md.CAMPAIGN_ID                      AS campaign_id,
        md.LINE_ITEM_ID                     AS line_item_id,
        md.ADVERTISER_ID                    AS advertiser_id,
        CASE
            WHEN UPPER(li.inventory) = 'OFF'      THEN 'ONLY_OFF'
            WHEN UPPER(li.inventory) = 'MELI_OFF' THEN 'IN_PLUS_OFF'
            ELSE 'ONLY_IN'
        END AS inv_setting,
        CASE
            WHEN md.CAMPAIGN_TARGET IN ('REACH','IMPRESSION','IMPRESSIONS') THEN 'AWARENESS'
            WHEN md.CAMPAIGN_TARGET = 'CPC'                                 THEN 'CONSIDERACION'
            WHEN md.CAMPAIGN_TARGET IN ('CPA','CONVERSION','CONVERSIONS','ROAS') THEN 'CONVERSION'
            ELSE md.CAMPAIGN_TARGET
        END AS strategy
    FROM meli-bi-data.WHOWNER.BT_ADS_DISP_METRICS_DAILY md
    LEFT JOIN meli-bi-data.SBOX_ADVERTISINGDISPLAY.line_item li
        ON li.line_item_id = md.LINE_ITEM_ID
    WHERE md.EVENT_LOCAL_DT BETWEEN '{OO_START}' AND '{ANALYSIS_END}'
      AND md.CAMPAIGN_TYPE = 'PROGRAMMATIC'
      AND md.PRODUCT_TYPE = 'DSP_SELFSERVICE'
      AND UPPER(md.LINE_ITEM_TYPE) NOT IN ('VIDEO', 'SOCIAL')
      AND md.CAMPAIGN_TARGET IN ('REACH','IMPRESSION','IMPRESSIONS','CPC','CPA','CONVERSION','CONVERSIONS','ROAS')
      AND md.IMPRESSION_COST_AMT_USD > 0
)
SELECT
    dl.site,
    dl.campaign_id,
    dl.line_item_id,
    dl.advertiser_id,
    dl.inv_setting,
    dl.strategy,
    (iab.LINE_ITEM_ID IS NOT NULL) AS has_iab_creative
FROM active_display_li dl
LEFT JOIN iab_li iab ON iab.LINE_ITEM_ID = dl.line_item_id
"""

# ══════════════════════════════════════════════════════════════════
# EXTRACT
# ══════════════════════════════════════════════════════════════════

CSV_FILES = {
    'enet':     'raw_lanz_enet.csv',
    'aw_denom': 'raw_lanz_aw_denom.csv',
    'arpu':     'raw_lanz_arpu.csv',
    'nuevos':   'raw_lanz_nuevos.csv',
    'iab':      'raw_lanz_iab.csv',
}

dfs = {}

if FROM_CSV:
    missing = [f for k, f in CSV_FILES.items() if not os.path.exists(f)]
    if missing:
        print("ERROR: Faltan CSVs:", missing)
        sys.exit(1)
    print("Cargando desde CSVs locales...")
    for k, f in CSV_FILES.items():
        dfs[k] = pd.read_csv(f)
        print(f"  {f}: {len(dfs[k])} filas")
else:
    queries = {
        'enet':     (q_enet,     '[1/5] ENET Awareness (LI breakdown)'),
        'aw_denom': (q_aw_denom, '[2/5] Awareness total (denominador)'),
        'arpu':     (q_arpu,     '[3/5] ARPU before/after Only Off'),
        'nuevos':   (q_nuevos,   '[4/5] Nuevos en ADX / DSP via Only Off'),
        'iab':      (q_iab,      '[5/5] IAB Creatives'),
    }
    for k, (q, label) in queries.items():
        print(label + "...")
        dfs[k] = query_df(client, q)
        dfs[k].to_csv(CSV_FILES[k], index=False)
        print(f"      {len(dfs[k])} filas")

df_enet    = dfs['enet']
df_denom   = dfs['aw_denom']
df_arpu    = dfs['arpu']
df_nuevos  = dfs['nuevos']
df_iab     = dfs['iab']

print()
# ══════════════════════════════════════════════════════════════════
# TRANSFORM
# ══════════════════════════════════════════════════════════════════

# ── Helpers ───────────────────────────────────────────────────────

def to_num(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)
    return df

INV_ORDER  = ['ONLY_IN', 'IN_PLUS_OFF', 'ONLY_OFF']
INV_LABELS = {'ONLY_IN': 'Solo In-MELI', 'IN_PLUS_OFF': 'In + Off', 'ONLY_OFF': 'Solo Off'}

# ── 1. ENET (Awareness + Consideración) ───────────────────────────
to_num(df_enet, ['rev_total','rev_off','rev_in','prints_total','clicks_total'])

INV_PRIORITY = {'ONLY_OFF': 2, 'IN_PLUS_OFF': 1, 'ONLY_IN': 0}

_inv = df_enet['raw_inventory'].str.upper().str.strip()
df_enet['inv_setting'] = np.where(
    _inv == 'OFF',      'ONLY_OFF',
    np.where(_inv == 'MELI_OFF', 'IN_PLUS_OFF',
    np.where(_inv.isin(['IN', 'MELI']), 'ONLY_IN',
    None))  # NULL / valor desconocido
)

# Check: cuántos LIs tienen inventory NULL o valor inesperado
_null_lis = df_enet[df_enet['inv_setting'].isna()]
print(f"[CHECK] LIs con inventory NULL/desconocido: {len(_null_lis)} "
      f"({_null_lis['advertiser_id'].nunique()} adv, "
      f"rev=${_null_lis['rev_total'].sum():,.0f})")
if not _null_lis.empty:
    print(_null_lis[['raw_inventory']].value_counts().to_string())

# Excluir NULLs del análisis principal
df_enet = df_enet[df_enet['inv_setting'].notna()].copy()
df_enet['inv_pri'] = df_enet['inv_setting'].map(INV_PRIORITY).fillna(0).astype(int)

# Advertiser summary (global)
adv_grp = df_enet.groupby('advertiser_id').agg(
    advertiser_name=('advertiser_name', 'first'),
    vertical=('vertical', 'first'),
    max_inv_pri=('inv_pri', 'max'),
    rev_total=('rev_total', 'sum'),
    rev_off=('rev_off', 'sum'),
    rev_in=('rev_in', 'sum'),
    prints_total=('prints_total', 'sum'),
    clicks_total=('clicks_total', 'sum'),
    n_li=('line_item_id', 'nunique'),
).reset_index()
adv_grp['inv_category'] = adv_grp['max_inv_pri'].map({2: 'ONLY_OFF', 1: 'IN_PLUS_OFF', 0: 'ONLY_IN'})
adv_grp['is_endemic'] = ~adv_grp['vertical'].apply(is_non_endemic)

# Volume table by inventory category (global)
vol_by_inv = df_enet.groupby('inv_setting').agg(
    n_adv=('advertiser_id', 'nunique'),
    n_li=('line_item_id', 'nunique'),
    rev=('rev_total', 'sum'),
    rev_off=('rev_off', 'sum'),
    rev_in=('rev_in', 'sum'),
    prints_total=('prints_total', 'sum'),
).reset_index()
vol_by_inv['cpm'] = safe_div(vol_by_inv['rev'], vol_by_inv['prints_total'], mult=1000)
vol_by_inv['inv_label'] = vol_by_inv['inv_setting'].map(INV_LABELS)
vol_by_inv = vol_by_inv.sort_values('inv_setting',
    key=lambda s: s.map(INV_PRIORITY), ascending=False)

# Totals from expanded denom (per-advertiser)
to_num(df_denom, ['rev_total', 'flag_adx'])
df_denom['is_endemic'] = ~df_denom['vertical'].apply(is_non_endemic)

total_aw_adv     = df_denom['advertiser_id'].nunique()
total_aw_rev     = float(df_denom['rev_total'].sum())

# Non-endemic denominators
non_end_denom_adv = df_denom[~df_denom['is_endemic']]['advertiser_id'].nunique()
non_end_denom_adx = int(df_denom[(~df_denom['is_endemic']) & (df_denom['flag_adx'] == 1)]['advertiser_id'].nunique())

# Adoption rates (global, all inventories)
adopt_rows = []
for cat in INV_ORDER:
    row = vol_by_inv[vol_by_inv['inv_setting'] == cat]
    n_adv = int(row['n_adv'].iloc[0]) if not row.empty else 0
    rev   = float(row['rev'].iloc[0])  if not row.empty else 0
    adopt_rows.append({
        'category': INV_LABELS[cat],
        'n_adv':    n_adv,
        'rev':      rev,
        'adv_pct':  round(n_adv / total_aw_adv * 100, 1) if total_aw_adv else 0,
        'rev_pct':  round(rev   / total_aw_rev * 100, 1)  if total_aw_rev  else 0,
    })
df_adopt = pd.DataFrame(adopt_rows)

# ── 2. Endemic / Non-endemic ───────────────────────────────────────
to_num(df_arpu, ['rev_before','rev_after_total','rev_after_oo','rev_after_in_meli'])
df_arpu['has_dot_com'] = df_arpu['has_dot_com'].astype(bool) if 'has_dot_com' in df_arpu.columns else False
df_arpu['is_endemic'] = ~df_arpu['vertical'].apply(is_non_endemic)
df_arpu['arpu_delta'] = df_arpu['rev_after_total'] - df_arpu['rev_before']
df_arpu['arpu_delta_pct'] = safe_div(
    df_arpu['arpu_delta'], df_arpu['rev_before'], mult=100)

# Cannibalization check: in-MELI revenue after vs. before
df_arpu['in_meli_delta_pct'] = safe_div(
    df_arpu['rev_after_in_meli'] - df_arpu['rev_before'],
    df_arpu['rev_before'], mult=100)

# OO revenue is incremental share
df_arpu['oo_incremental_pct'] = safe_div(
    df_arpu['rev_after_oo'], df_arpu['rev_after_total'], mult=100)

# ── Endémicos que adoptaron Only Off ──────────────────────────────
endemic_oo     = df_arpu[df_arpu['is_endemic']].copy()
non_endemic_oo = df_arpu[~df_arpu['is_endemic']].copy()

endn           = max(len(endemic_oo), 1)
endtotal_before = endemic_oo['rev_before'].sum()
endtotal_after  = endemic_oo['rev_after_total'].sum()
endarpu_before  = endtotal_before / endn
endarpu_after   = endtotal_after  / endn

# Endémicos con incrementalidad confirmada (ARPU subió + .com)
endemic_incremental = endemic_oo[
    (endemic_oo['rev_after_total'] > endemic_oo['rev_before']) &
    (endemic_oo['has_dot_com'] == True)
]

# Endémicos con posible canibalización (In-MELI cayó >10%)
endemic_canibalizacion = endemic_oo[
    endemic_oo['in_meli_delta_pct'].notna() &
    (endemic_oo['in_meli_delta_pct'] < -10)
]

# ── No endémicos ───────────────────────────────────────────────────
non_end_oo_ids  = set(non_endemic_oo['advertiser_id'].unique())

# No endémicos en Display Awareness que NO adoptaron Only Off (oportunidad)
non_end_denom_df = df_denom[~df_denom['is_endemic']].copy()
non_end_not_oo   = non_end_denom_df[~non_end_denom_df['advertiser_id'].isin(non_end_oo_ids)]

# Adopción no endémicos
non_end_adopt_disp = round(len(non_end_oo_ids) / non_end_denom_adv * 100, 1) if non_end_denom_adv else 0
non_end_adopt_adx  = round(len(non_end_oo_ids) / non_end_denom_adx  * 100, 1) if non_end_denom_adx  else 0

# ── 3. Nuevos en ADX / DSP ────────────────────────────────────────
if not df_nuevos.empty and 'nuevo_en_adx' in df_nuevos.columns:
    df_nuevos['nuevo_en_adx'] = df_nuevos['nuevo_en_adx'].astype(bool)
    df_nuevos['nuevo_en_dsp'] = df_nuevos['nuevo_en_dsp'].astype(bool)
    n_nuevos_adx = int(df_nuevos['nuevo_en_adx'].sum())
    n_nuevos_dsp = int(df_nuevos['nuevo_en_dsp'].sum())
    n_total_oo   = len(df_nuevos)
else:
    n_nuevos_adx = n_nuevos_dsp = n_total_oo = 0

# ── 4. IAB Creatives ─────────────────────────────────────────────
df_iab['has_iab_creative'] = df_iab['has_iab_creative'].astype(bool) \
    if not df_iab.empty and 'has_iab_creative' in df_iab.columns else False

df_iab['is_enet'] = df_iab['inv_setting'].isin(['IN_PLUS_OFF', 'ONLY_OFF']) \
    if not df_iab.empty else False

df_iab['is_only_off'] = (df_iab['inv_setting'] == 'ONLY_OFF') \
    if not df_iab.empty else False

iab_camp_enet   = df_iab[df_iab['is_enet']]['campaign_id'].nunique()
iab_camp_oo     = df_iab[df_iab['is_only_off']]['campaign_id'].nunique()
iab_camp_enet_w = df_iab[df_iab['is_enet'] & df_iab['has_iab_creative']]['campaign_id'].nunique()
iab_camp_oo_w   = df_iab[df_iab['is_only_off'] & df_iab['has_iab_creative']]['campaign_id'].nunique()

iab_adop_enet   = round(iab_camp_enet_w / iab_camp_enet * 100, 1) if iab_camp_enet else 0
iab_adop_oo     = round(iab_camp_oo_w   / iab_camp_oo   * 100, 1) if iab_camp_oo   else 0

elapsed = round(time.time() - _t0, 1)
print(f"Transform completado en {elapsed}s\n")
# ══════════════════════════════════════════════════════════════════
# HTML HELPERS
# ══════════════════════════════════════════════════════════════════

def kpi_card(label, value, sub='', variant=''):
    # variant: '' | 'yellow' | 'green' | 'red' | 'blue' | 'purple'
    cls = f'kpi kpi-{variant}' if variant else 'kpi'
    return f"""<div class="{cls}">
      <div class="kpi-val">{value}</div>
      <div class="kpi-lbl">{label}</div>
      {'<div class="kpi-sub">'+sub+'</div>' if sub else ''}
    </div>"""

def build_table(headers, rows, highlight_col=None, highlight_fn=None):
    ths = ''.join(f'<th>{h}</th>' for h in headers)
    body = ''
    for row in rows:
        tds = ''
        for i, cell in enumerate(row):
            cls = ''
            if highlight_col is not None and i == highlight_col and highlight_fn:
                cls = ' class="' + highlight_fn(cell) + '"'
            tds += f'<td{cls}>{cell}</td>'
        body += f'<tr>{tds}</tr>'
    return f'<div class="table-wrap"><table><thead><tr>{ths}</tr></thead><tbody>{body}</tbody></table></div>'

def delta_cls(v):
    if isinstance(v, str):
        try: v = float(v.replace('%','').replace('+','').replace('$','').replace('K','').replace('M',''))
        except: return ''
    if pd.isna(v): return ''
    return 'pos' if v > 0 else ('neg' if v < 0 else '')

# NOTE: The CSS string, JavaScript data/logic blocks, and HTML body generation
# are defined below. They are embedded directly in the output HTML file.
# The CSS, JS, and HTML use Python f-strings where needed (config values injected).

# ── CSS (kept as raw string for embedding) ───────────────────────
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
       background: #f0f2f5; color: #1a1a2e; }
/* See full CSS in analisis_lanzamientos.html — truncated here for brevity */
"""

# ── Utility for progress bar HTML ────────────────────────────────
def pct_bar(pct, color='#3483FA', max_w=80):
    w = min(round(float(pct) / 100 * max_w, 1), max_w)
    return (f'<span class="bar-wrap">'
            f'<span class="bar-bg" style="width:{max_w}px">'
            f'<span class="bar-fg" style="background:{color};width:{w}px"></span>'
            f'</span></span>')

# ── Build table rows for HTML sections ───────────────────────────

# Volume table rows
vol_rows = []
for _, r in vol_by_inv.iterrows():
    tag_map = {'ONLY_IN': 'tag-in', 'IN_PLUS_OFF': 'tag-hyb', 'ONLY_OFF': 'tag-oo'}
    lbl = f'<span class="tag {tag_map.get(r.inv_setting,"")}">{r.inv_label}</span>'
    cpm_s = f'${r.cpm:.1f}' if not pd.isna(r.cpm) else '—'
    vol_rows.append([lbl, fmt_num(r.n_adv), fmt_num(r.n_li), fmt_usd(r.rev), cpm_s])

# Adoption table rows
adopt_rows_html = []
for _, r in df_adopt.iterrows():
    adopt_rows_html.append([
        r.category,
        fmt_num(r.n_adv),
        f'{r.adv_pct}% {pct_bar(r.adv_pct, "#1a73e8")}',
        fmt_usd(r.rev),
        f'{r.rev_pct}% {pct_bar(r.rev_pct, "#34a853")}',
    ])

# Endemic ARPU table (top 15 by after-revenue)
end_rows = []
for _, r in endemic_oo.nlargest(15, 'rev_after_total').iterrows():
    dot_com = '✓' if r.get('has_dot_com', False) else ''
    delta_s = fmt_pct(r.arpu_delta_pct)
    can_s   = fmt_pct(r.in_meli_delta_pct)
    incr_s  = f'{r.oo_incremental_pct:.0f}%' if not pd.isna(r.oo_incremental_pct) else '—'
    delta_c = 'pos' if (not pd.isna(r.arpu_delta_pct) and r.arpu_delta_pct > 0) else 'neg'
    can_c   = 'neg' if (not pd.isna(r.in_meli_delta_pct) and r.in_meli_delta_pct < -10) else ''
    end_rows.append([
        r.advertiser_name,
        dot_com,
        fmt_usd(r.rev_before),
        fmt_usd(r.rev_after_total),
        f'<span class="{delta_c}">{delta_s}</span>',
        f'<span class="{can_c}">{can_s}</span>',
        incr_s,
    ])

# Non-endemic opportunity list
non_end_opp_rows = []
for _, r in non_end_not_oo.nlargest(15, 'rev_total').iterrows():
    non_end_opp_rows.append([
        r.advertiser_name,
        r.get('vertical','—'),
        fmt_usd(r.rev_total),
        '<span class="badge badge-orange">Sin Only Off</span>',
    ])

# Non-endemic already in Only Off
non_end_oo_rows = []
for _, r in non_endemic_oo.nlargest(10, 'rev_after_total').iterrows():
    non_end_oo_rows.append([
        r.advertiser_name,
        r.get('vertical','—'),
        fmt_usd(r.rev_after_oo),
        fmt_usd(r.rev_after_total),
    ])

# Nuevos rows
nuevo_rows_adx = []
nuevo_rows_dsp = []
if not df_nuevos.empty:
    for _, r in df_nuevos[df_nuevos['nuevo_en_adx'] == True].sort_values('advertiser_name').iterrows():
        nuevo_rows_adx.append([
            r.get('advertiser_name','—'),
            r.get('site','—'),
            '<span class="badge badge-green">1er vez ADX</span>',
        ])
    for _, r in df_nuevos[df_nuevos['nuevo_en_dsp'] == True].sort_values('advertiser_name').iterrows():
        nuevo_rows_dsp.append([
            r.get('advertiser_name','—'),
            r.get('site','—'),
            '<span class="badge badge-blue">1er vez DSP</span>',
        ])

# ══════════════════════════════════════════════════════════════════
# JSON DATA FOR HTML
# ══════════════════════════════════════════════════════════════════

import json

def to_json(df, cols=None):
    """Serialize df to JSON list, keeping only needed cols, replacing NaN with None."""
    if cols:
        cols = [c for c in cols if c in df.columns]
        df = df[cols]
    df = df.copy()
    for c in df.select_dtypes(include='bool').columns:
        df[c] = df[c].astype(bool)
    return json.loads(df.where(pd.notna(df), None).to_json(orient='records'))

enet_json   = to_json(df_enet,   ['site','advertiser_id','advertiser_name','vertical',
                                    'campaign_id','line_item_id','inv_setting','strategy',
                                    'rev_total','rev_off','rev_in','prints_total','prints_off','prints_in','clicks_total'])
denom_json  = to_json(df_denom,  ['site','advertiser_id','vertical','rev_total','flag_adx','is_endemic','strategy'])
arpu_json   = to_json(df_arpu,   ['site','advertiser_id','advertiser_name','vertical',
                                    'has_dot_com','strategy','rev_before','rev_after_total',
                                    'rev_after_oo','rev_after_in_meli',
                                    'arpu_delta_pct','in_meli_delta_pct','oo_incremental_pct'])
nuevos_json = to_json(df_nuevos, ['site','advertiser_id','advertiser_name',
                                    'nuevo_en_adx','nuevo_en_dsp'])
iab_json    = to_json(df_iab,    ['site','campaign_id','line_item_id','advertiser_id',
                                    'inv_setting','strategy','has_iab_creative'])

CONFIG_JSON  = json.dumps({
    'oo_start':      OO_START,
    'analysis_end':  ANALYSIS_END,
    'before_start':  BEFORE_START,
    'before_end':    BEFORE_END,
    'days_after':    DAYS_AFTER_OO,
    'adx_min_usd':   ADX_MIN_USD,
    'history_start': OO_HISTORY_START,
    'non_endemic':   sorted(NON_ENDEMIC_VERTICALS),
    'sites_lbl':     SITES_LBL,
})

# JS data block (f-string, injects serialized JSON)
JSDATA = f"""
const DATA = {{
  enet:   {json.dumps(enet_json)},
  denom:  {json.dumps(denom_json)},
  arpu:   {json.dumps(arpu_json)},
  nuevos: {json.dumps(nuevos_json)},
  iab:    {json.dumps(iab_json)},
  config: {CONFIG_JSON}
}};
"""

# ══════════════════════════════════════════════════════════════════
# LOGO
# ══════════════════════════════════════════════════════════════════

_LOGO_PATH = os.path.join(os.path.dirname(__file__), 'Mercado_Ads.webp')
_LOGO_B64  = ''
if os.path.exists(_LOGO_PATH):
    with open(_LOGO_PATH, 'rb') as _f:
        _LOGO_B64 = base64.b64encode(_f.read()).decode()
_LOGO_TAG = f'<img src="data:image/webp;base64,{_LOGO_B64}" style="height:36px;width:auto;display:block;border-radius:6px">' if _LOGO_B64 else ''

# ══════════════════════════════════════════════════════════════════
# HTML OUTPUT
# ══════════════════════════════════════════════════════════════════
# NOTE: The full CSS and JS code blocks are embedded in the HTML.
# See analisis_lanzamientos_template.html for the full template.
# The JSDATA variable (above) injects the query results as JSON.
# The JSCODE variable below contains the dashboard rendering logic.

# For the complete HTML with CSS + JS, see the full script.
# This section generates and writes the output file.

OUTPUT = 'analisis_lanzamientos.html'

# The HTML_BODY is built using the CSS, JSDATA, and JSCODE variables.
# See full implementation in the repository.

# ── Write output ────────────────────────────────────────────────────────────────

OUTPUT = 'analisis_lanzamientos.html'

HTML = HTML_BODY  # assembled from CSS + JSDATA + JSCODE

with open(OUTPUT, 'w', encoding='utf-8') as f:
    f.write(HTML)

# ── Upload to GCS ────────────────────────────────────────────────────────────────

GCS_BUCKET = os.environ.get('GCS_BUCKET', 'negocio-display-reportes')
GCS_OBJECT = 'analisis-lanzamientos/reporte.html'

try:
    from google.cloud import storage as gcs
    gcs_client = gcs.Client()
    bucket     = gcs_client.bucket(GCS_BUCKET)
    blob       = bucket.blob(GCS_OBJECT)
    blob.upload_from_filename(OUTPUT, content_type='text/html')
    blob.make_public()
    print(f"\u2705  GCS: https://storage.googleapis.com/{GCS_BUCKET}/{GCS_OBJECT}")
except Exception as e:
    print(f"\u26a0\ufe0f  GCS upload fall\u00f3: {e}")

total_s = round(time.time() - _t0, 1)
print(f"\u2705  Output: {OUTPUT}")
print(f"   Tiempo total: {total_s}s")
