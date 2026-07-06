# sync_bucket.py
import os
import json
import unicodedata
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
COL_CE = "CE"

COL_POTENCIAL_CREDITO = "Potencial Credito"
COL_BANCO_ORIGEN = "BANCOS_ESTANDAR"

TZ = "America/Bogota"

PRIMERA_ASIGNACION_POR_NEGOCIADOR = 13

BANCOS_EXCLUIDOS_PRIMERA_ASIGNACION = [
    "davivienda",
    "finandina",
    "sistecredito",
    "serfinanza",
    "alkomprar",
    "caja social",
    "colsubsidio",
    "agaval",
    "comultrasan",
    "lulo",
    "nu",
]

UPDATE_COLS_FUNNEL = [
    "inserted_at_ultima",
    "end_ultima",
    "CATEGORIA_PRED_ultima",
    "payment_to_bank_ultima",
    "observations_ultima",
    "Descuento_Actualizacion",
    "Tipo de Actividad",
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
    "FASE",
    "STATUS",
    "_end_norm2",
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

def _sin_acentos(s):
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def _parse_date_series(x):
    dt = pd.to_datetime(x, errors="coerce")
    if dt.isna().mean() > 0.90:
        dt = pd.to_datetime(
            x.astype(str).str.replace("T", " ", regex=False),
            errors="coerce"
        )
    return dt

def _drop_excluded(df):
    excl = {_norm_key(c) for c in EXCLUDE_COLS_BUCKET}
    cols_to_drop = [c for c in df.columns if _norm_key(c) in excl]
    return df.drop(columns=cols_to_drop, errors="ignore")

def _filter_header_excluded(header):
    excl = {_norm_key(c) for c in EXCLUDE_COLS_BUCKET}
    return [c for c in header if _norm_key(c) not in excl]

def _valor_0_a_10_no_vacio(s):
    txt = str(s).strip()
    if txt == "" or txt.lower() in ["nan", "none", "null"]:
        return False

    txt = txt.replace(",", ".")
    val = pd.to_numeric(txt, errors="coerce")
    return pd.notna(val) and 0 <= float(val) <= 10

def _banco_es_excluido(banco):
    b = _sin_acentos(banco)
    return any(k in b for k in BANCOS_EXCLUIDOS_PRIMERA_ASIGNACION)

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
        raise RuntimeError("Falta MI_JSON.")

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
        return pd.DataFrame()
    df = pd.DataFrame(values[1:], columns=values[0])
    df.columns = [_norm_col(c) for c in df.columns]
    return df

def get_or_create_worksheet(gc, sheet_id, tab_name):
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows="3000", cols="60")
    return sh, ws

def ensure_columns(base_header, must_have):
    header = list(base_header)
    for c in must_have:
        if c not in header:
            header.append(c)
    return header

def apply_preferred_order(df, header):
    pref = [c for c in PREFERRED_ORDER if c in header]
    rest = [c for c in header if c not in pref]
    return pref + rest

def df_to_rows(df, header):
    return df.reindex(columns=header, fill_value="").astype(str).values.tolist()

def clear_monthly_fields_if_not_current_month(df_bucket, tz=TZ):
    if df_bucket.empty or "Fecha Actualizacion" not in df_bucket.columns:
        return df_bucket

    for c in MONTHLY_CLEAR_COLS:
        if c not in df_bucket.columns:
            continue

    now = pd.Timestamp.now(tz=tz)
    cur_y, cur_m = now.year, now.month

    dt = _parse_date_series(df_bucket["Fecha Actualizacion"])
    mask_old = dt.notna() & ~((dt.dt.year == cur_y) & (dt.dt.month == cur_m))

    if mask_old.any():
        for c in MONTHLY_CLEAR_COLS:
            if c in df_bucket.columns:
                df_bucket.loc[mask_old, c] = ""

    return df_bucket

def referencias_validas_primera_asignacion(df_base):
    if COL_POTENCIAL_CREDITO not in df_base.columns:
        return set()

    if COL_BANCO_ORIGEN not in df_base.columns:
        return set()

    tmp = df_base.copy()

    tmp["_potencial_ok"] = tmp[COL_POTENCIAL_CREDITO].apply(_valor_0_a_10_no_vacio)
    tmp["_banco_excluido"] = tmp[COL_BANCO_ORIGEN].apply(_banco_es_excluido)

    resumen = (
        tmp.groupby(COL_REF)
        .agg(
            tiene_potencial_ok=("_potencial_ok", "max"),
            todos_bancos_excluidos=("_banco_excluido", "min"),
        )
        .reset_index()
    )

    resumen = resumen[
        (resumen["tiene_potencial_ok"] == True) &
        (resumen["todos_bancos_excluidos"] == False)
    ]

    return set(resumen[COL_REF].astype(str).str.strip())

# =========================================================
# MAIN
# =========================================================
def main():
    gc = get_gspread_client()
    updates_in_bucket = 0

    df = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)
    if df.empty:
        print("Funnel vacío")
        return

    req = [COL_REF, COL_BUCKET, COL_NEGOCIADOR, COL_INSERTED_AT, COL_TIPO_ACT]
    for c in req:
        if c not in df.columns:
            raise RuntimeError(f"Falta columna {c} en Funnel")

    has_ce = COL_CE in df.columns

    df[COL_REF] = df[COL_REF].astype(str).str.strip()
    df[COL_NEGOCIADOR] = df[COL_NEGOCIADOR].astype(str).str.strip()
    df[COL_BUCKET] = pd.to_numeric(df[COL_BUCKET], errors="coerce")
    df = df[df[COL_BUCKET].notna()].copy()
    df[COL_BUCKET] = df[COL_BUCKET].astype(int)
    df["_inserted_dt"] = _parse_date_series(df[COL_INSERTED_AT])

    now = pd.Timestamp.now(tz=TZ)
    today = now.date()

    df_today = df[df["_inserted_dt"].dt.date == today].copy()
    if df_today.empty:
        print("Hoy no hay actividad")
        return

    df_quota = df_today.copy()
    tipo_q = df_quota[COL_TIPO_ACT].astype(str).str.upper().str.strip()

    df_quota["_peso"] = 2000
    df_quota.loc[tipo_q.eq("EFECTIVA"), "_peso"] = 4000

    quotas = df_quota.groupby(COL_NEGOCIADOR)["_peso"].sum().astype(int).to_dict()
    bucket_actual_max = int(df_today[COL_BUCKET].max())

    sh_b, ws_b = get_or_create_worksheet(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    values = ws_b.get_all_values()

    funnel_cols = [
        c for c in df.columns.tolist()
        if c != "_inserted_dt"
        and _norm_key(c) not in {_norm_key(x) for x in EXCLUDE_COLS_BUCKET}
    ]

    funnel_cols_bucket_names = [
        FUNNEL_TO_BUCKET_RENAME.get(c, c)
        for c in funnel_cols
        if _norm_key(FUNNEL_TO_BUCKET_RENAME.get(c, c)) not in {_norm_key(x) for x in EXCLUDE_COLS_BUCKET}
    ]

    desired_header = ensure_columns(funnel_cols_bucket_names, [COL_NUEVO])
    desired_header = _filter_header_excluded(desired_header)

    bucket_estaba_vacio = not values

    if bucket_estaba_vacio:
        tmp = pd.DataFrame(columns=desired_header)
        header = apply_preferred_order(tmp, desired_header)
        header = _filter_header_excluded(header)
        ws_b.update("A1", [header])
        df_bucket = pd.DataFrame(columns=header)
        current_header = header
    else:
        current_header = [_norm_col(c) for c in values[0]]
        current_header = [FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in current_header]
        current_header = _filter_header_excluded(current_header)

        rows = values[1:]
        df_bucket = pd.DataFrame(rows, columns=[_norm_col(c) for c in values[0]])
        df_bucket.columns = [_norm_col(c) for c in df_bucket.columns]
        df_bucket.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)
        df_bucket = _drop_excluded(df_bucket)

        # IMPORTANTE:
        # Si el Bucket ya tiene encabezados, NO agregamos columnas nuevas.
        header = current_header

        for c in header:
            if c not in df_bucket.columns:
                df_bucket[c] = ""

    removed_refs = set()

    if not df_bucket.empty and COL_REF in df_bucket.columns:
        df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()

        df_funnel_latest_bucket = (
            df.sort_values("_inserted_dt")
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

        refs_in_bucket = df_bucket[COL_REF].astype(str).str.strip()
        to_remove = []

        for ref in refs_in_bucket.unique().tolist():
            b_now = funnel_bucket_map.get(ref, None)
            if b_now is None or pd.isna(b_now):
                continue
            if int(b_now) > bucket_actual_max:
                to_remove.append(ref)

        if to_remove:
            removed_refs = set(to_remove)
            df_bucket = df_bucket[~df_bucket[COL_REF].isin(removed_refs)].copy()

            if COL_NUEVO in df_bucket.columns:
                df_bucket[COL_NUEVO] = ""

            df_bucket = _drop_excluded(df_bucket)

            ws_b.update(
                "A2",
                df_to_rows(df_bucket, header),
                value_input_option="USER_ENTERED"
            )

    if not df_bucket.empty and COL_NUEVO in df_bucket.columns:
        if df_bucket[COL_NUEVO].astype(str).str.strip().ne("").any():
            df_bucket[COL_NUEVO] = ""
            col = header.index(COL_NUEVO) + 1
            ws_b.update(
                f"{gspread.utils.rowcol_to_a1(2, col)}:"
                f"{gspread.utils.rowcol_to_a1(len(df_bucket)+1, col)}",
                [[v] for v in df_bucket[COL_NUEVO].astype(str).tolist()]
            )

    existing_refs = set()
    if not df_bucket.empty and COL_REF in df_bucket.columns:
        existing_refs = set(df_bucket[COL_REF].astype(str).str.strip().tolist())

    df_cand = df[~df[COL_REF].isin(existing_refs)].copy()
    if df_cand.empty:
        print(
            f"No hay referencias nuevas | Actualizaciones en Bucket: {updates_in_bucket} | "
            f"Refs removidas por cambio de bucket: {len(removed_refs)}"
        )
        return

    ref_priority = (
        df_cand.groupby(COL_REF)
        .agg(
            bucket_ref=(COL_BUCKET, "min"),
            inserted_min=("_inserted_dt", "min"),
            negociador=(COL_NEGOCIADOR, lambda x: x.mode().iloc[0] if not x.mode().empty else x.iloc[0]),
        )
        .reset_index()
        .sort_values(["negociador", "bucket_ref", "inserted_min"])
    )

    chosen_refs = []
    used = set(existing_refs)

    refs_validas_especial = referencias_validas_primera_asignacion(df_cand)

    # =========================================================
    # PRIMERA ASIGNACIÓN:
    # Hasta 13 referencias por negociador,
    # Potencial Credito no vacío y entre 0 y 10,
    # excluyendo referencias donde TODOS los bancos sean excluidos.
    # Solo se priorizan Bucket 0 a 10.
    # =========================================================
    primera_asignacion_refs = []

    for neg in sorted(ref_priority["negociador"].dropna().unique().tolist()):
        sub = ref_priority[
            (ref_priority["negociador"] == neg) &
            (ref_priority[COL_REF].isin(refs_validas_especial)) &
            (ref_priority["bucket_ref"].between(0, 10)) &
            (~ref_priority[COL_REF].isin(used))
        ].copy()

        take = sub.head(PRIMERA_ASIGNACION_POR_NEGOCIADOR)

        if not take.empty:
            refs = take[COL_REF].tolist()
            primera_asignacion_refs.extend(refs)
            chosen_refs.extend(refs)
            used.update(refs)

    # =========================================================
    # FLUJO NORMAL:
    # Después de agotar la primera asignación, sigue como antes.
    # =========================================================
    for neg, quota in quotas.items():
        sub = ref_priority[ref_priority["negociador"] == neg]
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
        print(
            f"No se asignó nada hoy | Actualizaciones en Bucket: {updates_in_bucket} | "
            f"Refs removidas por cambio de bucket: {len(removed_refs)}"
        )
        return

    df_out = df[df[COL_REF].isin(chosen_refs)].copy()
    df_out[COL_NUEVO] = "Nuevo"
    df_out = df_out.drop(columns=["_inserted_dt"], errors="ignore")

    df_out.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)
    df_out = _drop_excluded(df_out)
    df_out = clear_monthly_fields_if_not_current_month(df_out, tz=TZ)

    # IMPORTANTE:
    # No agregamos columnas nuevas al Bucket.
    # Solo se escriben las columnas que ya están en el header.
    ws_b.append_rows(
        df_to_rows(df_out, header),
        value_input_option="USER_ENTERED"
    )

    print(
        f"Bucket_actual_max(hoy): {bucket_actual_max} | "
        f"Refs removidas por cambio de bucket: {len(removed_refs)} | "
        f"Actualizaciones en Bucket: {updates_in_bucket} | "
        f"Refs primera asignación especial: {len(set(primera_asignacion_refs))} | "
        f"Referencias asignadas total: {len(set(chosen_refs))} | "
        f"Filas insertadas: {len(df_out)} | "
        f"CE en Funnel: {'SI' if has_ce else 'NO'}"
    )

if __name__ == "__main__":
    main()
