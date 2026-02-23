# sync_bucket_updates_only.py
# ---------------------------------------------------------
# FINALIDAD (AJUSTADA):
# - NO asigna nuevas referencias
# - NO borra filas
# - SOLO actualiza (para Id deuda ya existentes en Bucket):
#   1) Descuento Requerido (desde Funnel: Descuento)
#   2) CE (si existe en Funnel)
#   3) Ahorro total (solo si es distinto Funnel vs Bucket)
#   4) Por cobrar (solo si es distinto Funnel vs Bucket)   ✅ NUEVO
#   5) Estas columnas “de actualización” (último registro en Funnel por inserted_at_ultima):
#      Descuento_Actualizacion, Fecha Actualizacion, Actualizado Por,
#      Categoria Actualizacion, Pago a Banco actualizacion, Observación, Tipo de Actividad
# - Mantiene el orden del header (PREFERRED_ORDER)
# - Regla mensual: si Fecha Actualizacion no es del mes actual, vacía esas 7 columnas
#
# FIX ZONA HORARIA (SIN DOBLE CONVERSIÓN):
# - NO convertimos df_latest["Fecha Actualizacion"] antes del merge.
# - Convertimos SOLO UNA VEZ al momento de actualizar Bucket.
# ---------------------------------------------------------

import os
import json
import numpy as np  # ✅ necesario para np.nan en _to_num_strict
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

# ✅ Ahorro total
COL_AHORRO_FUNNEL = "Ahorro total"
COL_AHORRO_BUCKET = "Ahorro total"

# ✅ NUEVO: Por cobrar (mismo nombre en Funnel y Bucket)
COL_POR_COBRAR_FUNNEL = "Por cobrar"
COL_POR_COBRAR_BUCKET = "Por cobrar"

# 🔧 Si tu Bucket históricamente quedó guardado como UTC "oculto" (sin Z),
# ponlo True para corregir histórico. Cuando ya quede bien, lo pones False.
ASSUME_BUCKET_DATES_ARE_UTC = False

FUNNEL_TO_BUCKET_RENAME = {
    "BANCOS_ESTANDAR": "Banco",
    "Descuento": "Descuento Requerido",
    "inserted_at_ultima": "Fecha Actualizacion",
    "end_ultima": "Actualizado Por",
    "CATEGORIA_PRED_ultima": "Categoria Actualizacion",
    "payment_to_bank_ultima": "Pago a Banco actualizacion",
    "observations_ultima": "Observación",
    # Ahorro total se queda igual
    # Por cobrar se queda igual
}

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

def _to_num_strict(v: pd.Series) -> pd.Series:
    """
    Convierte strings tipo:
      "100.000", "100,000", "100000", "100000,5" -> float
    Si no se puede, NaN.

    ✅ FIX: NO usar pd.NA dentro de Series dtype float64 (revienta). Usamos np.nan.
    """
    s = v.astype(str).str.strip()
    s = s.where(~_is_blank_series(s), pd.NA)

    s2 = s.astype("string")

    has_comma = s2.str.contains(",", regex=False)
    has_dot = s2.str.contains(".", regex=False)

    out = pd.Series(np.nan, index=s2.index, dtype="float64")

    mask_both = has_comma & has_dot
    if mask_both.any():
        tmp = s2[mask_both]
        last_is_comma = tmp.str.rfind(",") > tmp.str.rfind(".")
        tmp1 = tmp.where(
            ~last_is_comma,
            tmp.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        )
        tmp1 = tmp1.where(
            last_is_comma,
            tmp.str.replace(",", "", regex=False)
        )
        out.loc[mask_both] = pd.to_numeric(tmp1, errors="coerce")

    mask_only_comma = has_comma & ~has_dot
    if mask_only_comma.any():
        tmp = s2[mask_only_comma]
        many_commas = tmp.str.count(",") > 1
        tmp1 = tmp.where(many_commas, tmp.str.replace(",", ".", regex=False))
        tmp1 = tmp1.where(~many_commas, tmp.str.replace(",", "", regex=False))
        out.loc[mask_only_comma] = pd.to_numeric(tmp1, errors="coerce")

    mask_only_dot = has_dot & ~has_comma
    if mask_only_dot.any():
        tmp = s2[mask_only_dot]
        many_dots = tmp.str.count(r"\.") > 1
        tmp1 = tmp.where(many_dots, tmp.str.replace(".", "", regex=False))
        out.loc[mask_only_dot] = pd.to_numeric(tmp1, errors="coerce")

    mask_plain = ~has_comma & ~has_dot
    if mask_plain.any():
        out.loc[mask_plain] = pd.to_numeric(s2[mask_plain], errors="coerce")

    return out

def to_bogota_str(x: pd.Series, tz_local: str = TZ, assume_naive_is_utc: bool = True) -> pd.Series:
    s = x.astype(str).str.strip()
    s = s.str.replace("T", " ", regex=False)

    blank = _is_blank_series(s)
    out = pd.Series([""] * len(s), index=s.index, dtype="object")
    if blank.all():
        return out

    is_utc_hint = s.str.contains(r"(?:Z$|\+00:00|\+0000|UTC)", case=False, regex=True)

    if is_utc_hint.any():
        dt_utc = pd.to_datetime(s[is_utc_hint], errors="coerce", utc=True)
        dt_local = dt_utc.dt.tz_convert(tz_local)
        out.loc[is_utc_hint] = dt_local.dt.strftime("%Y-%m-%d %H:%M:%S").where(dt_local.notna(), "")

    if (~is_utc_hint).any():
        dt_naive = pd.to_datetime(s[~is_utc_hint], errors="coerce")
        if assume_naive_is_utc:
            dt_utc2 = dt_naive.dt.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
            dt_local2 = dt_utc2.dt.tz_convert(tz_local)
            out.loc[~is_utc_hint] = dt_local2.dt.strftime("%Y-%m-%d %H:%M:%S").where(dt_local2.notna(), "")
        else:
            out.loc[~is_utc_hint] = dt_naive.dt.strftime("%Y-%m-%d %H:%M:%S").where(dt_naive.notna(), "")

    out = out.where(~blank, "")
    return out

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

def apply_preferred_order(df: pd.DataFrame, header: list) -> list:
    pref = [c for c in PREFERRED_ORDER if c in df.columns]
    rest = [c for c in header if c in df.columns and c not in pref]
    final_header = pref + rest
    for c in df.columns:
        if c not in final_header:
            final_header.append(c)
    return final_header

def update_only_columns(ws, df_final: pd.DataFrame, header: list, cols_to_write: list):
    n = len(df_final)
    for col_name in cols_to_write:
        if col_name not in header or col_name not in df_final.columns:
            continue
        col_idx_1based = header.index(col_name) + 1
        start_cell = gspread.utils.rowcol_to_a1(2, col_idx_1based)
        end_cell = gspread.utils.rowcol_to_a1(n + 1, col_idx_1based)
        rng = f"{start_cell}:{end_cell}"

        ws.update(
            range_name=rng,
            values=[[v] for v in df_final[col_name].astype(str).tolist()],
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
    has_ahorro = COL_AHORRO_FUNNEL in df_funnel.columns
    has_por_cobrar = COL_POR_COBRAR_FUNNEL in df_funnel.columns  # ✅ NUEVO

    cols_needed = [COL_REF, COL_DESCUENTO_FUNNEL] + UPDATE_COLS_FUNNEL
    if has_ce:
        cols_needed.append(COL_CE)
    if has_ahorro:
        cols_needed.append(COL_AHORRO_FUNNEL)
    if has_por_cobrar:  # ✅ NUEVO
        cols_needed.append(COL_POR_COBRAR_FUNNEL)

    cols_needed = [c for c in cols_needed if c in df_funnel.columns]

    df_latest = (
        df_funnel.sort_values("_inserted_dt")
                 .groupby(COL_REF, as_index=False)
                 .tail(1)[cols_needed]
                 .copy()
    )

    df_latest.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    # Normalizar a string (resto)
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

    # Corregir histórico Bucket (si venía UTC oculto) -> lo dejamos ya en Colombia
    if "Fecha Actualizacion" in df_bucket.columns:
        df_bucket["Fecha Actualizacion"] = to_bogota_str(
            df_bucket["Fecha Actualizacion"],
            tz_local=TZ,
            assume_naive_is_utc=ASSUME_BUCKET_DATES_ARE_UTC
        )

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
    if has_ahorro:
        cols_target_bucket.append(COL_AHORRO_BUCKET)
    if has_por_cobrar:  # ✅ NUEVO
        cols_target_bucket.append(COL_POR_COBRAR_BUCKET)

    # asegurar columnas existen
    for c in cols_target_bucket:
        if c not in df_bucket.columns:
            df_bucket[c] = ""
        if c not in bucket_header:
            bucket_header.append(c)

    # Reordenar header (presentación)
    bucket_header = apply_preferred_order(df_bucket, bucket_header)
    ws_bucket.update(range_name="A1", values=[bucket_header])

    # -------- Merge para actualizar SOLO columnas target --------
    df_merged = df_bucket.merge(df_latest, on=COL_REF, how="left", suffixes=("", "__new"))

    updated_cells = 0
    for col in cols_target_bucket:
        col_new = f"{col}__new"
        if col_new not in df_merged.columns:
            continue

        old = df_merged[col]
        new = df_merged[col_new]

        # ✅ Caso especial: Fecha Actualizacion
        if col == "Fecha Actualizacion":
            old_cmp = to_bogota_str(old, tz_local=TZ, assume_naive_is_utc=False)
            new_cmp = to_bogota_str(new, tz_local=TZ, assume_naive_is_utc=True)

            mask_has_new = ~_is_blank_series(new_cmp)
            mask_diff = mask_has_new & (old_cmp.astype(str).ne(new_cmp.astype(str)))

            if mask_diff.any():
                df_merged.loc[mask_diff, col] = new_cmp[mask_diff]
                updated_cells += int(mask_diff.sum())

            df_merged.drop(columns=[col_new], inplace=True)
            continue

        # ✅ Caso especial: Ahorro total (comparación numérica tolerante)
        if col == COL_AHORRO_BUCKET:
            old_num = _to_num_strict(old)
            new_num = _to_num_strict(new)

            mask_has_new = new_num.notna()
            mask_diff = mask_has_new & (old_num.isna() | ((old_num - new_num).abs() > 1e-6))

            if mask_diff.any():
                df_merged.loc[mask_diff, col] = new.astype(str)[mask_diff]
                updated_cells += int(mask_diff.sum())

            df_merged.drop(columns=[col_new], inplace=True)
            continue

        # ✅ NUEVO: Caso especial Por cobrar (mismo tratamiento que Ahorro total)
        if col == COL_POR_COBRAR_BUCKET:
            old_num = _to_num_strict(old)
            new_num = _to_num_strict(new)

            mask_has_new = new_num.notna()
            mask_diff = mask_has_new & (old_num.isna() | ((old_num - new_num).abs() > 1e-6))

            if mask_diff.any():
                df_merged.loc[mask_diff, col] = new.astype(str)[mask_diff]
                updated_cells += int(mask_diff.sum())

            df_merged.drop(columns=[col_new], inplace=True)
            continue

        # --- resto igual (solo si new no está vacío y es distinto) ---
        mask_has_new = ~_is_blank_series(new)
        mask_diff = mask_has_new & (old.astype(str).ne(new.astype(str)))

        if mask_diff.any():
            df_merged.loc[mask_diff, col] = new.astype(str)[mask_diff]
            updated_cells += int(mask_diff.sum())

        df_merged.drop(columns=[col_new], inplace=True)

    # Formato final (ya en Colombia; NO convertir otra vez)
    if "Fecha Actualizacion" in df_merged.columns:
        df_merged["Fecha Actualizacion"] = to_bogota_str(
            df_merged["Fecha Actualizacion"],
            tz_local=TZ,
            assume_naive_is_utc=False
        )

    # -------- Regla mensual (NO incluye Ahorro total ni Por cobrar) --------
    df_merged = clear_monthly_fields_if_not_current_month(df_merged, tz=TZ)

    # -------- Escribir SOLO las columnas target --------
    update_only_columns(ws_bucket, df_merged, bucket_header, cols_target_bucket)

    print(
        f"OK | Celdas actualizadas (aprox): {updated_cells} | "
        f"Cols tocadas: {len(cols_target_bucket)} | "
        f"CE en Funnel: {'SI' if has_ce else 'NO'} | "
        f"Ahorro en Funnel: {'SI' if has_ahorro else 'NO'} | "
        f"Por cobrar en Funnel: {'SI' if has_por_cobrar else 'NO'} | "
        f"ASSUME_BUCKET_DATES_ARE_UTC: {ASSUME_BUCKET_DATES_ARE_UTC}"
    )

if __name__ == "__main__":
    main()
