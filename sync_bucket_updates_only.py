# sync_bucket_updates_only.py
# ---------------------------------------------------------
# FINALIDAD:
# - NO asigna nuevas referencias
# - NO borra por bucket
# - SOLO sincroniza columnas para referencias que YA existen en Bucket.
# - Renombra y reordena columnas (sin cambiar filas).
# - Incluye CE (si existe en Funnel) y la mantiene en su sitio.
# - Regla mensual: si "Fecha Actualizacion" NO es del mes actual,
#   vacía estas columnas:
#   Descuento_Actualizacion, Fecha Actualizacion, Actualizado Por,
#   Categoria Actualizacion, Pago a Banco actualizacion, Observación, Tipo de Actividad
# ---------------------------------------------------------

import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# =========================================================
# CONFIG
# =========================================================
FUNNEL_SHEET_ID = "1Bm1wjsfXdNDFrFTStQJHkERC08Eo21BwjZnu-WncibY"
FUNNEL_TAB_NAME = "Funnel"

BUCKET_SHEET_ID = "1qw77Q0BRAXfavNHzC53TT3usKDXRMp1pezAVG13qz3k"
BUCKET_TAB_NAME = "Bucket"

# Clave para cruzar Funnel vs Bucket (NO cambiar si no actualizas todo el flujo)
COL_REF = "Id deuda"
COL_INSERTED_AT = "inserted_at_ultima"

# Columna nueva a sincronizar (si existe en Funnel/Bucket)
COL_CE = "CE"

TZ = "America/Bogota"

# Columnas a mantener sincronizadas (nombres tal como están en Funnel hoy)
UPDATE_COLS = [
    "inserted_at_ultima",
    "end_ultima",
    "CATEGORIA_PRED_ultima",
    "payment_to_bank_ultima",
    "observations_ultima",
    "Descuento_Actualizacion",
    "Tipo de Actividad",
    "FASE",
    "STATUS",
]

# Columnas adicionales que quieres sincronizar (tal como están en Funnel hoy)
COL_AHORRO_TOTAL = "Ahorro total"
COL_POR_COBRAR = "Por cobrar"

UPDATE_COLS_EXTRA = [COL_AHORRO_TOTAL, COL_POR_COBRAR]

# Importante: CE entra a SYNC_COLS solo si existe en Funnel (se maneja abajo)
SYNC_COLS_BASE = UPDATE_COLS + UPDATE_COLS_EXTRA

# =========================================================
# PRESENTACIÓN EN BUCKET: renombres + orden deseado
# =========================================================
FUNNEL_TO_BUCKET_RENAME = {
    "BANCOS_ESTANDAR": "Banco",
    "Descuento": "Descuento Requerido",
    "inserted_at_ultima": "Fecha Actualizacion",
    "end_ultima": "Actualizado Por",
    "CATEGORIA_PRED_ultima": "Categoria Actualizacion",
    "payment_to_bank_ultima": "Pago a Banco actualizacion",
    "observations_ultima": "Observación",
    # CE se mantiene como "CE"
}

# ✅ Orden actual EXACTO (como lo reportaste)
PREFERRED_ORDER = [
    "Referencia",
    "Id deuda",
    "Cedula",
    "Nombre del cliente",
    "Negociador",
    "Banco",
    "D_BRAVO",
    "CE",  # <-- CE queda al lado de D_BRAVO (si existe)
    "Tipo de Liquidacion",
    "Ahorro total",
    "Por cobrar",
    "Meses en el Programa",
    "MORA",
    "Descuento Requerido",
    "Pago_banco_esperado",
    "Potencial",
    "Estructurable",
    "Potencial Credito",
    "Ingreso_esperado",
    "Descuento_Actualizacion",
    "Fecha Actualizacion",
    "Actualizado Por",
    "Categoria Actualizacion",
    "Pago a Banco actualizacion",
    "Observación",
    "Tipo de Actividad",
    "Mora_estructurado",
    "MORA_CREDITO",
    "ultimo contacto",
    "Bucket",
    "Nuevo",
    "FASE",
    "STATUS",
]

# =========================================================
# REGLA MENSUAL: columnas a vaciar si Fecha Actualizacion no es del mes actual
# (NOMBRES FINALES EN BUCKET)
# =========================================================
MONTHLY_CLEAR_COLS = [
    "Descuento_Actualizacion",
    "Fecha Actualizacion",
    "Actualizado Por",
    "Categoria Actualizacion",
    "Pago a Banco actualizacion",
    "Observación",
    "Tipo de Actividad",
]

# =========================================================
# HELPERS
# =========================================================
def _norm_col(s):
    return str(s).strip()

def _parse_date_series(x: pd.Series) -> pd.Series:
    dt = pd.to_datetime(x, errors="coerce")
    if dt.isna().mean() > 0.90:
        dt = pd.to_datetime(
            x.astype(str).str.replace("T", " ", regex=False),
            errors="coerce"
        )
    return dt

def clear_monthly_fields_if_not_current_month(df_bucket: pd.DataFrame, tz: str = TZ) -> pd.DataFrame:
    """
    Si 'Fecha Actualizacion' no pertenece al mes en curso, vacía MONTHLY_CLEAR_COLS en esas filas.
    No borra filas ni columnas: solo setea "".
    """
    if df_bucket.empty:
        return df_bucket
    if "Fecha Actualizacion" not in df_bucket.columns:
        return df_bucket

    for c in MONTHLY_CLEAR_COLS:
        if c not in df_bucket.columns:
            df_bucket[c] = ""

    now = pd.Timestamp.now(tz=tz)
    cur_y, cur_m = now.year, now.month

    dt = _parse_date_series(df_bucket["Fecha Actualizacion"])
    mask_old = dt.notna() & ~((dt.dt.year == cur_y) & (dt.dt.month == cur_m))

    if mask_old.any():
        for c in MONTHLY_CLEAR_COLS:
            df_bucket.loc[mask_old, c] = ""

    return df_bucket

def get_gspread_client():
    """
    Autenticación usando MI_JSON:
    - Colab: google.colab.userdata.get("MI_JSON")
    - GitHub/Local: os.environ["MI_JSON"]
    """
    mi_json = None

    try:
        from google.colab import userdata
        mi_json = userdata.get("MI_JSON")
    except Exception:
        pass

    if not mi_json:
        mi_json = os.environ.get("MI_JSON")

    if not mi_json:
        raise RuntimeError(
            "Falta MI_JSON. En Colab debe existir como secret (userdata), "
            "y en GitHub como Secret MI_JSON."
        )

    info = json.loads(mi_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def read_worksheet_as_df(gc, sheet_id, tab_name):
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab_name)
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame(), ws, []
    header = [_norm_col(c) for c in values[0]]
    df = pd.DataFrame(values[1:], columns=header)
    df.columns = [_norm_col(c) for c in df.columns]
    return df, ws, header

def ensure_columns(header, must_have):
    header = list(header)
    for c in must_have:
        if c not in header:
            header.append(c)
    return header

def df_to_rows(df, header):
    return df.reindex(columns=header, fill_value="").astype(str).values.tolist()

def apply_preferred_order(df: pd.DataFrame, header: list) -> list:
    pref = [c for c in PREFERRED_ORDER if c in df.columns]
    rest = [c for c in header if c in df.columns and c not in pref]
    final_header = pref + rest
    for c in df.columns:
        if c not in final_header:
            final_header.append(c)
    return final_header

# =========================================================
# MAIN
# =========================================================
def main():
    gc = get_gspread_client()

    # ------------------ Read Funnel ------------------
    df_funnel, _, _ = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)
    if df_funnel.empty:
        print("Funnel vacío. No hay nada para sincronizar.")
        return

    for c in [COL_REF, COL_INSERTED_AT]:
        if c not in df_funnel.columns:
            raise RuntimeError(f"Falta columna '{c}' en Funnel")

    df_funnel[COL_REF] = df_funnel[COL_REF].astype(str).str.strip()
    df_funnel["_inserted_dt"] = _parse_date_series(df_funnel[COL_INSERTED_AT])

    # CE solo se sincroniza si existe en Funnel
    SYNC_COLS = list(SYNC_COLS_BASE)
    has_ce = COL_CE in df_funnel.columns
    if has_ce and (COL_CE not in SYNC_COLS):
        SYNC_COLS.append(COL_CE)

    sync_cols_present_funnel = [c for c in SYNC_COLS if c in df_funnel.columns]
    if not sync_cols_present_funnel:
        print("Ninguna de las columnas SYNC_COLS existe en Funnel. No se hizo nada.")
        return

    sync_cols_present_bucket = [FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in sync_cols_present_funnel]

    df_latest = (
        df_funnel.sort_values("_inserted_dt")
                 .groupby(COL_REF, as_index=False)
                 .tail(1)[[COL_REF] + sync_cols_present_funnel]
                 .copy()
    )

    df_latest.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    for c in sync_cols_present_bucket:
        if c in df_latest.columns:
            df_latest[c] = df_latest[c].astype(str)

    # ------------------ Read Bucket ------------------
    df_bucket, ws_bucket, bucket_header = read_worksheet_as_df(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    if df_bucket.empty:
        print("Bucket vacío. No hay referencias existentes para actualizar.")
        return

    if COL_REF not in df_bucket.columns:
        raise RuntimeError(f"Falta columna '{COL_REF}' en Bucket")

    df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()

    # Migrar nombres viejos -> nuevos si aplica
    old_to_new_in_bucket = {k: v for k, v in FUNNEL_TO_BUCKET_RENAME.items() if k in df_bucket.columns}
    if old_to_new_in_bucket:
        df_bucket.rename(columns=old_to_new_in_bucket, inplace=True)
        bucket_header = [old_to_new_in_bucket.get(c, c) for c in bucket_header]

    # Asegurar que Bucket tenga columnas a sincronizar (y CE si aplica)
    new_header = ensure_columns(bucket_header, sync_cols_present_bucket)

    # Reordenar respetando orden actual exacto
    new_header = apply_preferred_order(df_bucket, new_header)

    # Asegurar columnas en df_bucket
    for c in new_header:
        if c not in df_bucket.columns:
            df_bucket[c] = ""

    if new_header != bucket_header:
        ws_bucket.update("A1", [new_header])
        bucket_header = new_header

    # ------------------ Merge & Update ------------------
    df_merged = df_bucket.merge(df_latest, on=COL_REF, how="left", suffixes=("", "__new"))

    updated_cells_approx = 0
    for c in sync_cols_present_bucket:
        c_new = f"{c}__new"
        if c_new not in df_merged.columns:
            continue

        old = df_merged[c].astype(str) if c in df_merged.columns else pd.Series([""] * len(df_merged))
        new = df_merged[c_new].astype(str)

        mask_has_new = new.ne("nan")
        mask_diff = mask_has_new & old.ne(new)

        if mask_diff.any():
            df_merged.loc[mask_diff, c] = new[mask_diff]
            updated_cells_approx += int(mask_diff.sum())

        df_merged.drop(columns=[c_new], inplace=True)

    # ✅ Regla mensual
    df_merged = clear_monthly_fields_if_not_current_month(df_merged, tz=TZ)

    # Header final (por si entraron nuevas cols) + orden exacto
    bucket_header = ensure_columns(bucket_header, [c for c in df_merged.columns if c not in bucket_header])
    bucket_header = apply_preferred_order(df_merged, bucket_header)

    # Nota: aunque "updated_cells_approx" sea 0, la regla mensual puede haber limpiado celdas,
    # así que reescribimos siempre para garantizar consistencia.
    ws_bucket.update("A1", [bucket_header])
    ws_bucket.update("A2", df_to_rows(df_merged, bucket_header), value_input_option="USER_ENTERED")

    print(
        f"OK | Referencias en Bucket: {df_bucket[COL_REF].nunique()} | "
        f"Columnas sincronizadas (presentes en Funnel): {len(sync_cols_present_funnel)} | "
        f"Actualizaciones (aprox por filas tocadas): {updated_cells_approx} | "
        f"CE en Funnel: {'SI' if has_ce else 'NO'}"
    )

if __name__ == "__main__":
    main()
