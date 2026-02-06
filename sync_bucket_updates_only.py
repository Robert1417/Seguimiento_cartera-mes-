# sync_bucket_updates_only.py
# ---------------------------------------------------------
# FINALIDAD (AJUSTADA):
# - NO asigna nuevas referencias
# - NO borra filas
# - SOLO actualiza (para Id deuda ya existentes en Bucket):
#   1) Descuento Requerido (desde Funnel: Descuento)
#   2) CE (si existe en Funnel)
#   3) Estas columnas “de actualización” (último registro en Funnel por inserted_at_ultima):
#      Descuento_Actualizacion, Fecha Actualizacion, Actualizado Por,
#      Categoria Actualizacion, Pago a Banco actualizacion, Observación, Tipo de Actividad
# - Mantiene el orden del header (PREFERRED_ORDER)
# - Regla mensual: si Fecha Actualizacion no es del mes actual, vacía esas 7 columnas
# ---------------------------------------------------------

import os
import json
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

FUNNEL_SHEET_ID = "1Bm1wjsfXdNDFrFTStQJHkERC08Eo21BwjZnu-WncibY"
FUNNEL_TAB_NAME = "Funnel"

BUCKET_SHEET_ID = "1qw77Q0BRAXfavNHzC53TT3usKDXRMp1pezAVG13qz3k"
BUCKET_TAB_NAME = "Bucket"

COL_REF = "Id deuda"
COL_INSERTED_AT = "inserted_at_ultima"
TZ = "America/Bogota"

COL_CE = "CE"
COL_DESCUENTO_FUNNEL = "Descuento"
COL_DESCUENTO_BUCKET = "Descuento Requerido"

FUNNEL_TO_BUCKET_RENAME = {
    "BANCOS_ESTANDAR": "Banco",
    "Descuento": "Descuento Requerido",
    "inserted_at_ultima": "Fecha Actualizacion",
    "end_ultima": "Actualizado Por",
    "CATEGORIA_PRED_ultima": "Categoria Actualizacion",
    "payment_to_bank_ultima": "Pago a Banco actualizacion",
    "observations_ultima": "Observación",
}

# Orden actual exacto
PREFERRED_ORDER = [
    "Referencia",
    "Id deuda",
    "Cedula",
    "Nombre del cliente",
    "Negociador",
    "Banco",
    "D_BRAVO",
    "CE",
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

MONTHLY_CLEAR_COLS = [
    "Descuento_Actualizacion",
    "Fecha Actualizacion",
    "Actualizado Por",
    "Categoria Actualizacion",
    "Pago a Banco actualizacion",
    "Observación",
    "Tipo de Actividad",
]

# Columnas de Funnel que alimentan esas 7 (ojo: algunas se renombran)
UPDATE_COLS_FUNNEL = [
    "Descuento_Actualizacion",
    "inserted_at_ultima",
    "end_ultima",
    "CATEGORIA_PRED_ultima",
    "payment_to_bank_ultima",
    "observations_ultima",
    "Tipo de Actividad",
]

def _norm_col(s):
    return str(s).strip()

def _parse_date_series(x: pd.Series) -> pd.Series:
    dt = pd.to_datetime(x, errors="coerce")
    if dt.isna().mean() > 0.90:
        dt = pd.to_datetime(x.astype(str).str.replace("T", " ", regex=False), errors="coerce")
    return dt

def _is_blank_series(s: pd.Series) -> pd.Series:
    s2 = s.astype(str).str.strip()
    return s.isna() | s2.eq("") | s2.str.lower().isin(["nan", "none", "nat"])

def clear_monthly_fields_if_not_current_month(df_bucket: pd.DataFrame, tz: str = TZ) -> pd.DataFrame:
    if df_bucket.empty or "Fecha Actualizacion" not in df_bucket.columns:
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
    mi_json = None
    try:
        from google.colab import userdata
        mi_json = userdata.get("MI_JSON")
    except Exception:
        pass

    if not mi_json:
        mi_json = os.environ.get("MI_JSON")

    if not mi_json:
        raise RuntimeError("Falta MI_JSON en secrets (Colab) o env (GitHub).")

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

def apply_preferred_order(df: pd.DataFrame, header: list) -> list:
    pref = [c for c in PREFERRED_ORDER if c in df.columns]
    rest = [c for c in header if c in df.columns and c not in pref]
    final_header = pref + rest
    for c in df.columns:
        if c not in final_header:
            final_header.append(c)
    return final_header

def update_only_columns(ws, df_final: pd.DataFrame, header: list, cols_to_write: list):
    """
    Escribe SOLO columnas específicas (no reescribe toda la tabla).
    """
    n = len(df_final)
    for col_name in cols_to_write:
        if col_name not in header or col_name not in df_final.columns:
            continue
        col_idx_1based = header.index(col_name) + 1
        start_cell = gspread.utils.rowcol_to_a1(2, col_idx_1based)
        end_cell = gspread.utils.rowcol_to_a1(n + 1, col_idx_1based)
        rng = f"{start_cell}:{end_cell}"
        ws.update(
            rng,
            [[v] for v in df_final[col_name].astype(str).tolist()],
            value_input_option="USER_ENTERED"
        )

def main():
    gc = get_gspread_client()

    # -------- Funnel --------
    df_funnel, _, _ = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)
    if df_funnel.empty:
        print("Funnel vacío.")
        return

    for c in [COL_REF, COL_INSERTED_AT, COL_DESCUENTO_FUNNEL]:
        if c not in df_funnel.columns:
            raise RuntimeError(f"Falta columna '{c}' en Funnel")

    df_funnel[COL_REF] = df_funnel[COL_REF].astype(str).str.strip()
    df_funnel["_inserted_dt"] = _parse_date_series(df_funnel[COL_INSERTED_AT])

    has_ce = COL_CE in df_funnel.columns

    # columnas Funnel que vamos a traer del “último estado”
    cols_needed = [COL_REF, COL_DESCUENTO_FUNNEL] + UPDATE_COLS_FUNNEL
    if has_ce:
        cols_needed.append(COL_CE)

    cols_needed = [c for c in cols_needed if c in df_funnel.columns]

    df_latest = (
        df_funnel.sort_values("_inserted_dt")
                 .groupby(COL_REF, as_index=False)
                 .tail(1)[cols_needed]
                 .copy()
    )

    # Renombrar a nombres Bucket
    df_latest.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    # Normalizar a string
    for c in df_latest.columns:
        if c != COL_REF:
            df_latest[c] = df_latest[c].astype(str)

    # -------- Bucket --------
    df_bucket, ws_bucket, bucket_header = read_worksheet_as_df(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    if df_bucket.empty:
        print("Bucket vacío.")
        return

    if COL_REF not in df_bucket.columns:
        raise RuntimeError(f"Falta columna '{COL_REF}' en Bucket")

    df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()

    # asegurar columnas objetivo existan en Bucket (solo para poder escribir)
    cols_target_bucket = [
        COL_DESCUENTO_BUCKET,
        "Fecha Actualizacion",
        "Actualizado Por",
        "Categoria Actualizacion",
        "Pago a Banco actualizacion",
        "Observación",
        "Tipo de Actividad",
        "Descuento_Actualizacion",
    ]
    if has_ce:
        cols_target_bucket.append("CE")

    for c in cols_target_bucket:
        if c not in df_bucket.columns:
            df_bucket[c] = ""
        if c not in bucket_header:
            bucket_header.append(c)

    # Reordenar header (presentación)
    bucket_header = apply_preferred_order(df_bucket, bucket_header)
    ws_bucket.update("A1", [bucket_header])

    # -------- Merge para actualizar SOLO columnas target --------
    df_merged = df_bucket.merge(df_latest, on=COL_REF, how="left", suffixes=("", "__new"))

    updated_cells = 0
    for col in cols_target_bucket:
        col_new = f"{col}__new"
        if col_new not in df_merged.columns:
            continue

        old = df_merged[col]
        new = df_merged[col_new]

        # solo si Funnel trae algo válido (no vacío/nan)
        mask_has_new = ~_is_blank_series(new)
        # actualiza si bucket está vacío o distinto
        mask_diff = mask_has_new & (old.astype(str).ne(new.astype(str)))

        if mask_diff.any():
            df_merged.loc[mask_diff, col] = new.astype(str)[mask_diff]
            updated_cells += int(mask_diff.sum())

        df_merged.drop(columns=[col_new], inplace=True)

    # -------- Regla mensual --------
    df_merged = clear_monthly_fields_if_not_current_month(df_merged, tz=TZ)

    # -------- Escribir SOLO las columnas target (incluyendo las 7 + descuento + CE) --------
    update_only_columns(ws_bucket, df_merged, bucket_header, cols_target_bucket)

    print(
        f"OK | Celdas actualizadas (aprox): {updated_cells} | "
        f"Cols tocadas: {len(cols_target_bucket)} | CE en Funnel: {'SI' if has_ce else 'NO'}"
    )

if __name__ == "__main__":
    main()
