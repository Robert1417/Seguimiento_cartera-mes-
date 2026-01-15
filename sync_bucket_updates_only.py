# sync_bucket_updates_only.py
# ---------------------------------------------------------
# FINALIDAD:
# - NO asigna nuevas referencias
# - NO borra por bucket
# - SOLO sincroniza columnas (UPDATE_COLS + "Ahorro total" + "Por cobrar")
#   para referencias que YA existen en la hoja Bucket.
# Ideal para correr cada 30 min (workflow separado).
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

COL_REF = "Id deuda"
COL_INSERTED_AT = "inserted_at_ultima"

# Tus columnas a mantener sincronizadas
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

# Nuevas columnas adicionales que quieres sincronizar
COL_AHORRO_TOTAL = "Ahorro total"
COL_POR_COBRAR = "Por cobrar"

UPDATE_COLS_EXTRA = [COL_AHORRO_TOTAL, COL_POR_COBRAR]
SYNC_COLS = UPDATE_COLS + UPDATE_COLS_EXTRA

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

def get_gspread_client():
    """
    Autenticación usando MI_JSON:
    - Colab: google.colab.userdata.get("MI_JSON")
    - GitHub/Local: os.environ["MI_JSON"]
    """
    mi_json = None

    # 1) Colab
    try:
        from google.colab import userdata
        mi_json = userdata.get("MI_JSON")
    except Exception:
        pass

    # 2) GitHub / Local
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

    # Validaciones mínimas
    for c in [COL_REF, COL_INSERTED_AT]:
        if c not in df_funnel.columns:
            raise RuntimeError(f"Falta columna '{c}' en Funnel")

    df_funnel[COL_REF] = df_funnel[COL_REF].astype(str).str.strip()
    df_funnel["_inserted_dt"] = _parse_date_series(df_funnel[COL_INSERTED_AT])

    # Qué columnas realmente existen en Funnel para sincronizar (las que falten se ignoran)
    sync_cols_present = [c for c in SYNC_COLS if c in df_funnel.columns]
    if not sync_cols_present:
        print("Ninguna de las columnas SYNC_COLS existe en Funnel. No se hizo nada.")
        return

    # “Último estado” por referencia según inserted_dt
    df_latest = (
        df_funnel.sort_values("_inserted_dt")
                 .groupby(COL_REF, as_index=False)
                 .tail(1)[[COL_REF] + sync_cols_present]
                 .copy()
    )
    # Normalizar a string (Sheets se lleva bien con strings)
    for c in sync_cols_present:
        df_latest[c] = df_latest[c].astype(str)

    # ------------------ Read Bucket ------------------
    df_bucket, ws_bucket, bucket_header = read_worksheet_as_df(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    if df_bucket.empty:
        print("Bucket vacío. No hay referencias existentes para actualizar.")
        return

    if COL_REF not in df_bucket.columns:
        raise RuntimeError(f"Falta columna '{COL_REF}' en Bucket")

    df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()

    # Asegurar que Bucket tenga las columnas a sincronizar (para escribir sin error)
    new_header = ensure_columns(bucket_header, sync_cols_present)
    if new_header != bucket_header:
        ws_bucket.update("A1", [new_header])
        bucket_header = new_header
        # Asegurar columnas en df_bucket también
        for c in bucket_header:
            if c not in df_bucket.columns:
                df_bucket[c] = ""

    # ------------------ Merge & Update ------------------
    before = df_bucket.copy()

    # Merge left: conserva todas las filas del bucket
    df_merged = df_bucket.merge(df_latest, on=COL_REF, how="left", suffixes=("", "__new"))

    # Actualizar solo donde exista dato nuevo (no NaN)
    updated_cells_approx = 0
    for c in sync_cols_present:
        c_new = f"{c}__new"
        if c_new not in df_merged.columns:
            continue

        old = df_merged[c].astype(str)
        new = df_merged[c_new].astype(str)

        # “no update” si new es "nan" (porque no existía en Funnel para esa ref)
        mask_has_new = new.ne("nan")
        mask_diff = mask_has_new & old.ne(new)

        if mask_diff.any():
            df_merged.loc[mask_diff, c] = new[mask_diff]
            updated_cells_approx += int(mask_diff.sum())

        df_merged.drop(columns=[c_new], inplace=True)

    # Si no cambió nada, no reescribir
    if updated_cells_approx == 0:
        print("No hubo cambios en las columnas sincronizadas. (0 actualizaciones)")
        return

    # ------------------ Write back Bucket (full rewrite) ------------------
    # Es la forma más robusta para evitar actualizar celda por celda.
    ws_bucket.update("A2", df_to_rows(df_merged, bucket_header), value_input_option="USER_ENTERED")

    print(
        f"OK | Referencias en Bucket: {df_bucket[COL_REF].nunique()} | "
        f"Columnas sincronizadas (presentes en Funnel): {len(sync_cols_present)} | "
        f"Actualizaciones (aprox por filas tocadas): {updated_cells_approx}"
    )

if __name__ == "__main__":
    main()
