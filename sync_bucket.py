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

# Columnas clave (POR REFERENCIA)
COL_REF = "Referencia"
COL_BUCKET = "Bucket"
COL_NEGOCIADOR = "Negociador"
COL_INSERTED_AT = "inserted_at_ultima"
COL_TIPO_ACT = "Tipo de Actividad"
COL_STATUS = "STATUS"
COL_NUEVO = "Nuevo"

TZ = "America/Bogota"

# Columnas que quieres mantener sincronizadas (aunque la referencia ya exista en Bucket)
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

    req = [
        COL_REF, COL_BUCKET, COL_NEGOCIADOR,
        COL_INSERTED_AT, COL_TIPO_ACT, COL_STATUS
    ]
    for c in req:
        if c not in df.columns:
            raise RuntimeError(f"Falta columna {c} en Funnel")

    df[COL_REF] = df[COL_REF].astype(str).str.strip()
    df[COL_NEGOCIADOR] = df[COL_NEGOCIADOR].astype(str).str.strip()
    df[COL_BUCKET] = pd.to_numeric(df[COL_BUCKET], errors="coerce")
    df = df[df[COL_BUCKET].notna()].copy()
    df[COL_BUCKET] = df[COL_BUCKET].astype(int)

    df["_inserted_dt"] = _parse_date_series(df[COL_INSERTED_AT])

    today = pd.Timestamp.now(tz=TZ).date()
    df_today = df[df["_inserted_dt"].dt.date == today].copy()
    if df_today.empty:
        print("Hoy no hay actividad")
        # OJO: aun si hoy no hay actividad, igual podrías querer sincronizar columnas.
        # Si quieres que igual sincronice, comenta este return.
        return

    # ------------------ Cupos ponderados (triple) ------------------
    tipo = df_today[COL_TIPO_ACT].astype(str).str.upper().str.strip()
    status = df_today[COL_STATUS].astype(str).str.upper().str.strip()

    df_today["_peso"] = 3
    df_today.loc[tipo.eq("EFECTIVA"), "_peso"] = 6
    df_today.loc[tipo.eq("EFECTIVA") & status.eq("LIQUIDADO"), "_peso"] = 9

    quotas = (
        df_today.groupby(COL_NEGOCIADOR)["_peso"]
        .sum().astype(int).to_dict()
    )

    # ------------------ Bucket sheet ------------------
    sh_b, ws_b = get_or_create_worksheet(gc, BUCKET_SHEET_ID, BUCKET_TAB_NAME)
    values = ws_b.get_all_values()

    funnel_cols = [c for c in df.columns.tolist() if c != "_inserted_dt"]
    desired_header = ensure_columns(funnel_cols, [COL_NUEVO])
    desired_header = ensure_columns(desired_header, UPDATE_COLS)  # asegurar columnas a sincronizar

    if not values:
        # Hoja nueva
        header = desired_header
        ws_b.update("A1", [header])
        df_bucket = pd.DataFrame(columns=header)
        current_header = header
    else:
        current_header = [_norm_col(c) for c in values[0]]
        header = ensure_columns(current_header, desired_header)

        # si cambió el header, lo actualizamos
        if header != current_header:
            ws_b.update("A1", [header])

        rows = values[1:]
        df_bucket = pd.DataFrame(rows, columns=current_header)
        df_bucket.columns = [_norm_col(c) for c in df_bucket.columns]

        # Asegurar columnas nuevas en df_bucket
        for c in header:
            if c not in df_bucket.columns:
                df_bucket[c] = ""

    # =========================================================
    # 1) SINCRONIZAR columnas dinámicas para referencias existentes
    # =========================================================
    updates_in_bucket = 0

    if not df_bucket.empty and COL_REF in df_bucket.columns:
        df_bucket[COL_REF] = df_bucket[COL_REF].astype(str).str.strip()
        existing_refs = set(df_bucket[COL_REF].tolist())

        # Asegurar que Funnel tenga las UPDATE_COLS (si alguna no existe, se ignora)
        update_cols_present = [c for c in UPDATE_COLS if c in df.columns]

        if update_cols_present and existing_refs:
            # Tomar "último estado" por referencia según _inserted_dt
            df_latest = (
                df[df[COL_REF].isin(existing_refs)]
                .sort_values("_inserted_dt")
                .groupby(COL_REF, as_index=False)
                .tail(1)
                [[COL_REF] + update_cols_present]
                .copy()
            )
            latest_map = df_latest.set_index(COL_REF)[update_cols_present].astype(str).to_dict(orient="index")

            # Actualizar filas de Bucket: para cada referencia, setear esas columnas
            for c in update_cols_present:
                if c not in df_bucket.columns:
                    df_bucket[c] = ""

            # Comparación y update
            for ref in existing_refs:
                if ref not in latest_map:
                    continue
                mask = df_bucket[COL_REF].eq(ref)
                for c, new_val in latest_map[ref].items():
                    old_vals = df_bucket.loc[mask, c].astype(str)
                    # si hay al menos una diferencia, actualizamos TODAS las filas de esa referencia
                    if (old_vals != str(new_val)).any():
                        df_bucket.loc[mask, c] = str(new_val)
                        updates_in_bucket += int(mask.sum())

            # Reescribir Bucket completo (solo si hubo cambios)
            if updates_in_bucket > 0:
                # También aprovechamos para asegurar que "Nuevo" quede vacío antes del append
                if COL_NUEVO in df_bucket.columns:
                    df_bucket[COL_NUEVO] = ""

                ws_b.update("A2", df_to_rows(df_bucket, header), value_input_option="USER_ENTERED")

    # ------------------ limpiar "Nuevo" (si no reescribimos arriba) ------------------
    # Si ya reescribimos, "Nuevo" quedó vacío y esto no hace falta.
    # Pero lo dejamos como "seguro", sin costo grande.
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
        print(f"No hay referencias nuevas | Actualizaciones en Bucket: {updates_in_bucket}")
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
        print(f"No se asignó nada hoy | Actualizaciones en Bucket: {updates_in_bucket}")
        return

    # ------------------ Insertar (todas las filas de esas referencias) ------------------
    df_out = df[df[COL_REF].isin(chosen_refs)].copy()
    df_out[COL_NUEVO] = "Nuevo"

    # Quitamos auxiliar
    df_out = df_out.drop(columns=["_inserted_dt"], errors="ignore")

    # Append usando el header final del bucket
    ws_b.append_rows(df_to_rows(df_out, header), value_input_option="USER_ENTERED")

    print(
        f"Actualizaciones en Bucket (celdas/fila tocadas aprox): {updates_in_bucket} | "
        f"Referencias asignadas: {len(set(chosen_refs))} | "
        f"Filas insertadas: {len(df_out)}"
    )


if __name__ == "__main__":
    main()
