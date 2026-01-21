# sync_bucket.py
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

# Columnas clave (POR REFERENCIA) EN FUNNEL
COL_REF = "Referencia"
COL_BUCKET = "Bucket"
COL_NEGOCIADOR = "Negociador"
COL_INSERTED_AT = "inserted_at_ultima"
COL_TIPO_ACT = "Tipo de Actividad"
COL_STATUS = "STATUS"
COL_NUEVO = "Nuevo"

TZ = "America/Bogota"

# Columnas a sincronizar (NOMBRES COMO ESTÁN EN FUNNEL)
UPDATE_COLS_FUNNEL = [
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
}

PREFERRED_ORDER = [
    "Referencia",
    "Id deuda",
    "Cedula",
    "Nombre del cliente",
    "Negociador",
    "Banco",
    "D_BRAVO",
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
    "tipo_fila",
    "Negociador liquidacion",
    "Por?",
    "Bucket",
    "Nuevo",
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
    """Devuelve header con todas las columnas de must_have agregadas al final si faltan."""
    header = list(base_header)
    for c in must_have:
        if c not in header:
            header.append(c)
    return header

def apply_preferred_order(df: pd.DataFrame, header: list) -> list:
    """
    Header final:
    - Primero columnas en PREFERRED_ORDER que existan
    - Luego el resto (sin duplicar)
    - Luego cualquier columna extra del DF
    """
    pref = [c for c in PREFERRED_ORDER if c in df.columns]
    rest = [c for c in header if c in df.columns and c not in pref]
    final_header = pref + rest
    for c in df.columns:
        if c not in final_header:
            final_header.append(c)
    return final_header

def df_to_rows(df, header):
    return df.reindex(columns=header, fill_value="").astype(str).values.tolist()

# =========================================================
# MAIN
# =========================================================
def main():
    gc = get_gspread_client()

    # ------------------ Funnel ------------------
    df = read_worksheet_as_df(gc, FUNNEL_SHEET_ID, FUNNEL_TAB_NAME)
    if df.empty:
        print("Funnel vacío")
        return

    req = [COL_REF, COL_BUCKET, COL_NEGOCIADOR, COL_INSERTED_AT, COL_TIPO_ACT, COL_STATUS]
    for c in req:
        if c not in df.columns:
            raise RuntimeError(f"Falta columna {c} en Funnel")

    df[COL_REF] = df[COL_REF].astype(str).str.strip()
    df[COL_NEGOCIADOR] = df[COL_NEGOCIADOR].astype(str).str.strip()
    df[COL_BUCKET] = pd.to_numeric(df[COL_BUCKET], errors="coerce")
    df = df[df[COL_BUCKET].notna()].copy()
    df[COL_BUCKET] = df[COL_BUCKET].astype(int)

    df["_inserted_dt"] = _parse_date_series(df[COL_INSERTED_AT])

    # =========================================================
    # ACTIVIDAD DEL DÍA (para ejecutar flujo) vs CUPOS (regla nueva)
    # - df_today: SOLO HOY (mantiene bucket_actual_max y regla de ejecución)
    # - df_quota: HOY + (AYER solo EFECTIVA & LIQUIDADO) para calcular quotas
    # =========================================================
    now = pd.Timestamp.now(tz=TZ)
    today = now.date()
    yesterday = (now - pd.Timedelta(days=1)).date()

    df_today = df[df["_inserted_dt"].dt.date == today].copy()
    if df_today.empty:
        print("Hoy no hay actividad")
        return

    # Dataset especial para cupos:
    # - HOY completo
    # - AYER solo EFECTIVA + LIQUIDADO
    df_yesterday = df[df["_inserted_dt"].dt.date == yesterday].copy()
    if not df_yesterday.empty:
        tipo_y = df_yesterday[COL_TIPO_ACT].astype(str).str.upper().str.strip()
        status_y = df_yesterday[COL_STATUS].astype(str).str.upper().str.strip()
        df_yesterday_special = df_yesterday[tipo_y.eq("EFECTIVA") & status_y.eq("LIQUIDADO")].copy()
    else:
        df_yesterday_special = df_yesterday  # vacío

    df_quota = pd.concat([df_today, df_yesterday_special], ignore_index=True)

    # ------------------ Cupos ponderados ------------------
    tipo_q = df_quota[COL_TIPO_ACT].astype(str).str.upper().str.strip()
    status_q = df_quota[COL_STATUS].astype(str).str.upper().str.strip()

    df_quota["_peso"] = 2
    df_quota.loc[tipo_q.eq("EFECTIVA"), "_peso"] = 4
    df_quota.loc[tipo_q.eq("EFECTIVA") & status_q.eq("LIQUIDADO"), "_peso"] = 6

    quotas = (
        df_quota.groupby(COL_NEGOCIADOR)["_peso"]
        .sum().astype(int).to_dict()
    )

    # bucket_actual_max se mantiene SOLO con HOY (como venía)
    bucket_actual_max = int(df_today[COL_BUCKET].max())

    # ------------------ Bucket sheet ------------------
    sh_b, ws_b = get_or_create_worksheet(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    values = ws_b.get_all_values()

    # Columnas del funnel (pero renombradas a como quieres en Bucket)
    funnel_cols = [c for c in df.columns.tolist() if c != "_inserted_dt"]
    funnel_cols_bucket_names = [FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in funnel_cols]

    # Header deseado: lo que venga del funnel (con rename) + Nuevo
    desired_header = ensure_columns(funnel_cols_bucket_names, [COL_NUEVO])

    if not values:
        tmp = pd.DataFrame(columns=desired_header)
        header = apply_preferred_order(tmp, desired_header)
        ws_b.update("A1", [header])
        df_bucket = pd.DataFrame(columns=header)
        current_header = header
    else:
        current_header = [_norm_col(c) for c in values[0]]
        current_header = [FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in current_header]

        rows = values[1:]
        df_bucket = pd.DataFrame(rows, columns=[_norm_col(c) for c in values[0]])
        df_bucket.columns = [_norm_col(c) for c in df_bucket.columns]
        df_bucket.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

        header = ensure_columns(current_header, desired_header)

        for c in header:
            if c not in df_bucket.columns:
                df_bucket[c] = ""

        header = apply_preferred_order(df_bucket, header)

        if header != current_header:
            ws_b.update("A1", [header])

    # =========================================================
    # LIMPIEZA POR CAMBIO DE BUCKET
    # =========================================================
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

            ws_b.update("A2", df_to_rows(df_bucket, header), value_input_option="USER_ENTERED")

    # =========================================================
    # SINCRONIZAR columnas dinámicas para referencias existentes (post-limpieza)
    # =========================================================
    updates_in_bucket = 0

    if not df_bucket.empty and COL_REF in df_bucket.columns:
        df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()
        existing_refs = set(df_bucket[COL_REF].tolist())

        update_cols_present_funnel = [c for c in UPDATE_COLS_FUNNEL if c in df.columns]
        update_cols_present_bucket = [FUNNEL_TO_BUCKET_RENAME.get(c, c) for c in update_cols_present_funnel]

        if update_cols_present_funnel and existing_refs:
            df_latest = (
                df[df[COL_REF].isin(existing_refs)]
                .sort_values("_inserted_dt")
                .groupby(COL_REF, as_index=False)
                .tail(1)[[COL_REF] + update_cols_present_funnel]
                .copy()
            )

            df_latest.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

            latest_map = (
                df_latest.set_index(COL_REF)[update_cols_present_bucket]
                .astype(str)
                .to_dict(orient="index")
            )

            for c in update_cols_present_bucket:
                if c not in df_bucket.columns:
                    df_bucket[c] = ""
                    if c not in header:
                        header.append(c)

            for ref in existing_refs:
                if ref not in latest_map:
                    continue
                mask = df_bucket[COL_REF].eq(ref)
                for c, new_val in latest_map[ref].items():
                    old_vals = df_bucket.loc[mask, c].astype(str)
                    if (old_vals != str(new_val)).any():
                        df_bucket.loc[mask, c] = str(new_val)
                        updates_in_bucket += int(mask.sum())

            if updates_in_bucket > 0:
                if COL_NUEVO in df_bucket.columns:
                    df_bucket[COL_NUEVO] = ""

                header = apply_preferred_order(df_bucket, header)
                ws_b.update("A1", [header])
                ws_b.update("A2", df_to_rows(df_bucket, header), value_input_option="USER_ENTERED")

    # ------------------ limpiar "Nuevo" (seguro) ------------------
    if not df_bucket.empty and COL_NUEVO in df_bucket.columns:
        if df_bucket[COL_NUEVO].astype(str).str.strip().ne("").any():
            df_bucket[COL_NUEVO] = ""
            col = header.index(COL_NUEVO) + 1
            ws_b.update(
                f"{gspread.utils.rowcol_to_a1(2, col)}:"
                f"{gspread.utils.rowcol_to_a1(len(df_bucket)+1, col)}",
                [[v] for v in df_bucket[COL_NUEVO].astype(str).tolist()]
            )

    # Referencias ya existentes (no se reasignan nunca)
    existing_refs = set()
    if not df_bucket.empty and COL_REF in df_bucket.columns:
        existing_refs = set(df_bucket[COL_REF].astype(str).str.strip().tolist())

    # ------------------ Referencias candidatas ------------------
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

    for neg, quota in quotas.items():
        sub = ref_priority[ref_priority["negociador"] == neg]
        remaining = int(quota)

        for b in [0, 1, 2, 3, 4, 5]:
            if remaining <= 0:
                break
            sb = sub[(sub["bucket_ref"] == b) & (~sub[COL_REF].isin(used))]
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

    # ------------------ Insertar (todas las filas de esas referencias) ------------------
    df_out = df[df[COL_REF].isin(chosen_refs)].copy()
    df_out[COL_NUEVO] = "Nuevo"
    df_out = df_out.drop(columns=["_inserted_dt"], errors="ignore")

    df_out.rename(columns=FUNNEL_TO_BUCKET_RENAME, inplace=True)

    header = ensure_columns(header, df_out.columns.tolist())
    header = apply_preferred_order(
        pd.concat([df_bucket, df_out], ignore_index=True) if not df_bucket.empty else df_out,
        header
    )
    ws_b.update("A1", [header])

    ws_b.append_rows(df_to_rows(df_out, header), value_input_option="USER_ENTERED")

    print(
        f"Bucket_actual_max(hoy): {bucket_actual_max} | "
        f"Refs removidas por cambio de bucket: {len(removed_refs)} | "
        f"Actualizaciones en Bucket (celdas/fila tocadas aprox): {updates_in_bucket} | "
        f"Referencias asignadas: {len(set(chosen_refs))} | "
        f"Filas insertadas: {len(df_out)}"
    )

if __name__ == "__main__":
    main()
