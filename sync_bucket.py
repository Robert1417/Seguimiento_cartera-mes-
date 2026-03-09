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

# Columnas clave
COL_REF = "Referencia"
COL_BUCKET = "Bucket"
COL_NEGOCIADOR = "Negociador"
COL_INSERTED_AT = "inserted_at_ultima"
COL_TIPO_ACT = "Tipo de Actividad"
COL_PB_60 = "PB Ideal 60 meses"
COL_NUEVO = "Nuevo"
COL_CE = "CE"

TZ = "America/Bogota"

UPDATE_COLS_FUNNEL = [
    "inserted_at_ultima",
    "end_ultima",
    "CATEGORIA_PRED_ultima",
    "payment_to_bank_ultima",
    "observations_ultima",
    "Descuento_Actualizacion",
    "Tipo de Actividad",
    "PB Ideal 48 meses",
    "PB Ideal 60 meses",
]

EXCLUDE_COLS_BUCKET = [
    "tipo_fila",
    "Negociador liquidacion",
    "Por?",
    "MORAEstructurable",
    "Ahorro medio",
    "Estado Deuda",
    "sub_estado_deuda",
    "estado_reparadora",
    "sub_estado_reparadora",
    "Priority_level",
    "Ultimo contacto",
    "ultimo contacto",
    "fecha mensaje",
    "Ingreso_funnel",
    "tiene_obs",
    "es_este_mes",
    "tiene_liquidado_historico",
]

FUNNEL_TO_BUCKET_RENAME = {
    "BANCOS_ESTANDAR": "Banco",
    "Descuento": "Descuento Requerido",
    "inserted_at_ultima": "Fecha Actualizacion",
    "end_ultima": "Actualizado Por",
    "CATEGORIA_PRED_ultima": "Categoria Actualizacion",
    "payment_to_bank_ultima": "Pago a Banco actualizacion",
    "observations_ultima": "Observación",
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
    "Bucket",
    "Nuevo",
    "PB Ideal 48 meses",
    "PB Ideal 60 meses",
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


# =========================================================
# HELPERS
# =========================================================
def _norm_col(s):
    return str(s).strip()


def _norm_key(s):
    return str(s).strip().lower()


def _parse_date_series(x):
    dt = pd.to_datetime(x, errors="coerce")

    if dt.isna().mean() > 0.90:
        dt = pd.to_datetime(
            x.astype(str).str.replace("T", " ", regex=False),
            errors="coerce",
        )

    return dt


def _drop_excluded(df):
    excl = {_norm_key(c) for c in EXCLUDE_COLS_BUCKET}

    cols = [c for c in df.columns if _norm_key(c) in excl]

    if cols:
        df = df.drop(columns=cols, errors="ignore")

    return df


def get_gspread_client():

    mi_json = os.environ.get("MI_JSON")

    if not mi_json:
        raise RuntimeError("Falta MI_JSON")

    info = json.loads(mi_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)

    return gspread.authorize(creds)


def read_worksheet_as_df(gc, sheet_id, tab):

    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet(tab)

    values = ws.get_all_values()

    if not values:
        return pd.DataFrame()

    df = pd.DataFrame(values[1:], columns=values[0])
    df.columns = [_norm_col(c) for c in df.columns]

    return df


def get_or_create_worksheet(gc, sheet_id, tab):

    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(tab)

    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab, rows="3000", cols="60")

    return sh, ws


def ensure_columns(base_header, must):

    header = list(base_header)

    for c in must:

        if c not in header:
            header.append(c)

    return header


def apply_preferred_order(df, header):

    pref = [c for c in PREFERRED_ORDER if c in df.columns]

    rest = [c for c in header if c in df.columns and c not in pref]

    final = pref + rest

    for c in df.columns:

        if c not in final:
            final.append(c)

    return final


def df_to_rows(df, header):
    return df.reindex(columns=header, fill_value="").astype(str).values.tolist()


# =========================================================
# MAIN
# =========================================================
def main():

    gc = get_gspread_client()

    df = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)

    if df.empty:
        print("Funnel vacío")
        return

    required = [
        COL_REF,
        COL_BUCKET,
        COL_NEGOCIADOR,
        COL_INSERTED_AT,
        COL_TIPO_ACT,
        COL_PB_60,
    ]

    for c in required:
        if c not in df.columns:
            raise RuntimeError(f"Falta columna {c}")

    df[COL_REF] = df[COL_REF].astype(str).str.strip()
    df[COL_NEGOCIADOR] = df[COL_NEGOCIADOR].astype(str).str.strip()

    df[COL_BUCKET] = pd.to_numeric(df[COL_BUCKET], errors="coerce")

    df = df[df[COL_BUCKET].notna()].copy()

    df[COL_BUCKET] = df[COL_BUCKET].astype(int)

    df["_inserted_dt"] = _parse_date_series(df[COL_INSERTED_AT])

    now = pd.Timestamp.now(tz=TZ)

    today = now.date()

    df_today = df[df["_inserted_dt"].dt.date == today]

    if df_today.empty:
        print("Hoy no hay actividad")
        return

    tipo = df_today[COL_TIPO_ACT].astype(str).str.upper().str.strip()
    pb60 = df_today[COL_PB_60].astype(str).str.upper().str.strip()

    df_today["_peso"] = 1

    df_today.loc[tipo.eq("EFECTIVA"), "_peso"] = 2

    df_today.loc[tipo.eq("EFECTIVA") & pb60.eq("LIQUIDADO"), "_peso"] = 3

    quotas = (
        df_today.groupby(COL_NEGOCIADOR)["_peso"].sum().astype(int).to_dict()
    )

    bucket_actual_max = int(df_today[COL_BUCKET].max())

    sh_b, ws_b = get_or_create_worksheet(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)

    values = ws_b.get_all_values()

    funnel_cols = [
        c
        for c in df.columns
        if c != "_inserted_dt"
        and _norm_key(c) not in {_norm_key(x) for x in EXCLUDE_COLS_BUCKET}
    ]

    bucket_names = [
        FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in funnel_cols
    ]

    header = ensure_columns(bucket_names, [COL_NUEVO])

    if not values:

        ws_b.update("A1", [header])

        df_bucket = pd.DataFrame(columns=header)

    else:

        df_bucket = pd.DataFrame(values[1:], columns=values[0])

    existing_refs = set()

    if not df_bucket.empty and COL_REF in df_bucket.columns:
        existing_refs = set(df_bucket[COL_REF].astype(str).str.strip())

    df_new = df[~df[COL_REF].isin(existing_refs)].copy()

    if df_new.empty:
        print("No hay nuevas referencias")
        return

    chosen = []

    for neg, quota in quotas.items():

        sub = df_new[df_new[COL_NEGOCIADOR] == neg]

        chosen.extend(sub.head(quota)[COL_REF].tolist())

    if not chosen:
        print("Nada asignado")
        return

    df_out = df[df[COL_REF].isin(chosen)].copy()

    df_out[COL_NUEVO] = "Nuevo"

    df_out = df_out.drop(columns=["_inserted_dt"], errors="ignore")

    df_out.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    header = apply_preferred_order(df_out, header)

    ws_b.update("A1", [header])

    ws_b.append_rows(df_to_rows(df_out, header), value_input_option="USER_ENTERED")

    print("Filas insertadas:", len(df_out))


if __name__ == "__main__":
    main()
