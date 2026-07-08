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

COL_REF = "Referencia"
COL_BUCKET = "Bucket"
COL_NEGOCIADOR = "Negociador"
COL_INSERTED_AT = "inserted_at_ultima"
COL_TIPO_ACT = "Tipo de Actividad"
COL_NUEVO = "Nuevo"

TZ = "America/Bogota"

FUNNEL_TO_BUCKET_RENAME = {
    "BANCOS_ESTANDAR": "Banco",
    "Descuento": "Descuento Requerido",
    "inserted_at_ultima": "Fecha Actualizacion",
    "end_ultima": "Actualizado Por",
    "CATEGORIA_PRED_ultima": "Categoria Actualizacion",
    "payment_to_bank_ultima": "Pago a Banco actualizacion",
    "observations_ultima": "Observación",
}

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

def _parse_date_series(x):
    dt = pd.to_datetime(x, errors="coerce")
    if dt.isna().mean() > 0.90:
        dt = pd.to_datetime(
            x.astype(str).str.replace("T", " ", regex=False),
            errors="coerce"
        )
    return dt

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
        raise RuntimeError("Falta MI_JSON en secrets.")

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

def get_or_create_worksheet(gc, sheet_id, tab_name):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="3000", cols="60")
    return ws

def df_to_rows(df, header):
    return df.reindex(columns=header, fill_value="").astype(str).values.tolist()

def clear_monthly_fields_if_not_current_month(df_bucket, tz=TZ):
    if df_bucket.empty:
        return df_bucket

    if "Fecha Actualizacion" not in df_bucket.columns:
        return df_bucket

    now = pd.Timestamp.now(tz=tz)
    cur_y, cur_m = now.year, now.month

    dt = _parse_date_series(df_bucket["Fecha Actualizacion"])

    mask_old = dt.notna() & ~(
        (dt.dt.year == cur_y) &
        (dt.dt.month == cur_m)
    )

    cols_to_clear = [c for c in MONTHLY_CLEAR_COLS if c in df_bucket.columns]

    if mask_old.any() and cols_to_clear:
        df_bucket.loc[mask_old, cols_to_clear] = ""

    return df_bucket

# =========================================================
# MAIN
# =========================================================
def main():
    gc = get_gspread_client()

    # ------------------ Leer Funnel ------------------
    df_funnel, _, _ = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)

    if df_funnel.empty:
        print("Funnel vacío")
        return

    required = [COL_REF, COL_BUCKET, COL_NEGOCIADOR, COL_INSERTED_AT]

    for col in required:
        if col not in df_funnel.columns:
            raise RuntimeError(f"Falta columna '{col}' en Funnel")

    df_funnel[COL_REF] = df_funnel[COL_REF].astype(str).str.strip()
    df_funnel[COL_NEGOCIADOR] = df_funnel[COL_NEGOCIADOR].astype(str).str.strip()

    df_funnel[COL_BUCKET] = pd.to_numeric(df_funnel[COL_BUCKET], errors="coerce")
    df_funnel = df_funnel[df_funnel[COL_BUCKET].notna()].copy()
    df_funnel[COL_BUCKET] = df_funnel[COL_BUCKET].astype(int)

    df_funnel["_inserted_dt"] = _parse_date_series(df_funnel[COL_INSERTED_AT])

    # =========================================================
    # ACTIVIDAD RECIENTE: HOY, AYER O ANTEAYER
    # Cada actualización vale 10 nuevas.
    # Ya NO importa si es EFECTIVA o NO EFECTIVA.
    # =========================================================
    now = pd.Timestamp.now(tz=TZ)
    today = now.date()
    min_date = (now - pd.Timedelta(days=2)).date()

    df_recent = df_funnel[
        df_funnel["_inserted_dt"].dt.date.between(min_date, today)
    ].copy()

    if df_recent.empty:
        print("No hay actividad reciente de hoy, ayer o antier")
        return

    df_recent["_peso"] = 10

    quotas = (
        df_recent
        .groupby(COL_NEGOCIADOR)["_peso"]
        .sum()
        .astype(int)
        .to_dict()
    )

    bucket_actual_max = int(df_recent[COL_BUCKET].max())

    # ------------------ Leer Bucket ------------------
    ws_bucket = get_or_create_worksheet(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    values_bucket = ws_bucket.get_all_values()

    if not values_bucket:
        print("La hoja Bucket no tiene encabezados. Primero crea las columnas en destino.")
        return

    bucket_header = [_norm_col(c) for c in values_bucket[0]]
    bucket_rows = values_bucket[1:]

    df_bucket = pd.DataFrame(bucket_rows, columns=bucket_header)
    df_bucket.columns = [_norm_col(c) for c in df_bucket.columns]

    if COL_REF not in df_bucket.columns:
        raise RuntimeError(f"La hoja Bucket no tiene la columna '{COL_REF}'")

    df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()

    # =========================================================
    # LIMPIEZA POR CAMBIO DE BUCKET
    # Se mantiene: si la referencia en Funnel ya pasó a un bucket mayor
    # que el bucket máximo reciente, se remueve del Bucket.
    # =========================================================
    removed_refs = set()

    df_funnel_latest_bucket = (
        df_funnel
        .sort_values("_inserted_dt")
        .groupby(COL_REF, as_index=False)
        .tail(1)[[COL_REF, COL_BUCKET]]
        .copy()
    )

    funnel_bucket_map = dict(
        zip(
            df_funnel_latest_bucket[COL_REF].astype(str).str.strip(),
            pd.to_numeric(df_funnel_latest_bucket[COL_BUCKET], errors="coerce")
        )
    )

    to_remove = []

    for ref in df_bucket[COL_REF].astype(str).str.strip().unique().tolist():
        b_now = funnel_bucket_map.get(ref)

        if b_now is None or pd.isna(b_now):
            continue

        if int(b_now) > bucket_actual_max:
            to_remove.append(ref)

    if to_remove:
        removed_refs = set(to_remove)
        df_bucket = df_bucket[~df_bucket[COL_REF].isin(removed_refs)].copy()

    # ------------------ Limpiar columna Nuevo ------------------
    if COL_NUEVO in df_bucket.columns:
        df_bucket[COL_NUEVO] = ""

    # ------------------ Referencias existentes ------------------
    existing_refs = set(df_bucket[COL_REF].astype(str).str.strip().tolist())

    # ------------------ Candidatas nuevas ------------------
    df_cand = df_funnel[~df_funnel[COL_REF].isin(existing_refs)].copy()

    if df_cand.empty:
        if len(df_bucket) > 0:
            ws_bucket.update(
                "A2",
                df_to_rows(df_bucket, bucket_header),
                value_input_option="USER_ENTERED"
            )

        print(
            f"No hay referencias nuevas | "
            f"Refs removidas por cambio de bucket: {len(removed_refs)}"
        )
        return

    ref_priority = (
        df_cand
        .groupby(COL_REF)
        .agg(
            bucket_ref=(COL_BUCKET, "min"),
            inserted_min=("_inserted_dt", "min"),
            negociador=(
                COL_NEGOCIADOR,
                lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]
            ),
        )
        .reset_index()
        .sort_values(["negociador", "bucket_ref", "inserted_min"])
    )

    chosen_refs = []
    used = set(existing_refs)

    for neg, quota in quotas.items():
        sub = ref_priority[ref_priority["negociador"] == neg].copy()
        remaining = int(quota)

        for b in [0, 1, 2, 3, 4, 5]:
            if remaining <= 0:
                break

            sb = sub[
                (sub["bucket_ref"] == b) &
                (~sub[COL_REF].isin(used))
            ]

            take = sb.head(remaining)

            if not take.empty:
                refs = take[COL_REF].tolist()
                chosen_refs.extend(refs)
                used.update(refs)
                remaining -= len(refs)

    if not chosen_refs:
        if len(df_bucket) > 0:
            ws_bucket.update(
                "A2",
                df_to_rows(df_bucket, bucket_header),
                value_input_option="USER_ENTERED"
            )

        print(
            f"No se asignó nada | "
            f"Refs removidas por cambio de bucket: {len(removed_refs)}"
        )
        return

    # =========================================================
    # Insertar todas las filas de las referencias elegidas
    # IMPORTANTE:
    # No se agregan columnas nuevas.
    # Solo se llenan columnas que YA existen en Bucket.
    # =========================================================
    df_out = df_funnel[df_funnel[COL_REF].isin(chosen_refs)].copy()
    df_out = df_out.drop(columns=["_inserted_dt", "_peso"], errors="ignore")

    df_out.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    if COL_NUEVO in bucket_header:
        df_out[COL_NUEVO] = "Nuevo"

    df_out = clear_monthly_fields_if_not_current_month(df_out, tz=TZ)

    # Solo columnas existentes en destino
    df_out = df_out.reindex(columns=bucket_header, fill_value="")

    # También asegurar que df_bucket tenga solo columnas actuales de destino
    df_bucket = df_bucket.reindex(columns=bucket_header, fill_value="")

    # Reescribir cuerpo limpio si hubo removidas o Nuevo limpiado
    if len(df_bucket) > 0:
        ws_bucket.update(
            "A2",
            df_to_rows(df_bucket, bucket_header),
            value_input_option="USER_ENTERED"
        )

    # Agregar nuevas filas
    ws_bucket.append_rows(
        df_to_rows(df_out, bucket_header),
        value_input_option="USER_ENTERED"
    )

    print(
        f"Actividad considerada desde {min_date} hasta {today} | "
        f"Cada actualización = 10 nuevas | "
        f"Bucket_actual_max reciente: {bucket_actual_max} | "
        f"Refs removidas por cambio de bucket: {len(removed_refs)} | "
        f"Referencias asignadas: {len(set(chosen_refs))} | "
        f"Filas insertadas: {len(df_out)} | "
        f"No se agregaron columnas nuevas al Bucket"
    )

if __name__ == "__main__":
    main()
