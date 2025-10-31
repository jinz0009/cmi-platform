# -*- coding: utf-8 -*-
"""
Complete app_streamlit.py — integrated, fixed, with adjusted validation rules:
- safe_rerun() compatibility wrapper
- normalize_for_display / safe_st_dataframe to avoid pyarrow serialization errors
- detect_header_from_preview + auto_map_header for smart header detection
- Robust mapped_but_empty detection that handles Series/DataFrame/multi-source mappings
- "填写全局信息" flow: shows explicit button to expand the global form (uses mapping_csv from session),
  fills only empty cells, validates required fields, imports valid rows with download of invalid rows
- Admin delete flow verifies rowids, attempts archival, deletes and checks rowcount, then refreshes
- Validation rules updated:
  - "品牌" is no longer a mandatory field
  - Price rule: either "设备单价" or "人工包干单价" must be provided (at least one)
- Manual input forms updated to reflect that "品牌" is not required
"""
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib, io, re
from datetime import date

st.set_page_config(page_title="CMI 询价录入与查询平台", layout="wide")

# --- Compatibility helper: safe_rerun ---
def safe_rerun():
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
        try:
            from streamlit.runtime.scriptrunner import RerunException
            raise RerunException()
        except Exception:
            st.session_state["_needs_refresh"] = True
            st.warning("请手动刷新页面以查看最新状态（自动刷新在当前 Streamlit 版本不可用）。")
            return
    except Exception:
        st.session_state["_needs_refresh"] = True
        st.warning("无法自动重启，请手动刷新浏览器页面。")
        return

# Database engine (adjust URI for production)
engine = create_engine("sqlite:///quotation.db", connect_args={"check_same_thread": False})

# ============ Initialize DB (idempotent) ============
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT CHECK(role IN ('admin','user')),
        region TEXT
    )"""))
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS quotations (
        序号 TEXT,
        设备材料名称 TEXT NOT NULL,
        规格或型号 TEXT,
        描述 TEXT,
        品牌 TEXT,
        单位 TEXT,
        数量确认 REAL,
        报价品牌 TEXT,
        型号 TEXT,
        设备单价 REAL,
        设备小计 REAL,
        人工包干单价 REAL,
        人工包干小计 REAL,
        综合单价汇总 REAL,
        币种 TEXT,
        原厂品牌维保期限 TEXT,
        货期 TEXT,
        备注 TEXT,
        询价人 TEXT,
        项目名称 TEXT,
        供应商名称 TEXT,
        询价日期 TEXT,
        录入人 TEXT,
        地区 TEXT
    )"""))
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS misc_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        项目名称 TEXT,
        杂费类目 TEXT,
        金额 REAL,
        币种 TEXT,
        录入人 TEXT,
        地区 TEXT
    )"""))
    # default admin
    conn.execute(text("""
    INSERT OR IGNORE INTO users (username, password, role, region)
    VALUES ('admin', :pw, 'admin', 'All')"""), {"pw": hashlib.sha256("admin123".encode()).hexdigest()})

# ============ Config / Helpers ============
HEADER_SYNONYMS = {
    "序号":"序号","no":"序号","index":"序号",
    "设备材料名称":"设备材料名称","设备名称":"设备材料名称","material":"设备材料名称","name":"设备材料名称",
    "规格或型号":"规格或型号","规格":"规格或型号","model":"规格或型号","spec":"规格或型号",
    "描述":"描述","description":"描述",
    "品牌":"品牌","brand":"品牌",
    "单位":"单位","unit":"单位",
    "数量确认":"数量确认","数量":"数量确认","qty":"数量确认","quantity":"数量确认",
    "报价品牌":"报价品牌","报价":"报价品牌",
    "型号":"型号",
    "设备单价":"设备单价","单价":"设备单价","price":"设备单价",
    "设备小计":"设备小计","subtotal":"设备小计",
    "币种":"币种","currency":"币种",
    "询价人":"询价人","项目名称":"项目名称","供应商名称":"供应商名称","询价日期":"询价日期","录入人":"录入人","地区":"地区"
}
DB_COLUMNS = ["序号","设备材料名称","规格或型号","描述","品牌","单位","数量确认",
              "报价品牌","型号","设备单价","设备小计","人工包干单价","人工包干小计",
              "综合单价汇总","币种","原厂品牌维保期限","货期","备注",
              "询价人","项目名称","供应商名称","询价日期","录入人","地区"]

def auto_map_header(orig_header: str):
    if orig_header is None:
        return None
    h = str(orig_header).strip().lower()
    for k, v in HEADER_SYNONYMS.items():
        if h == k.lower():
            return v
    h_norm = re.sub(r"[\s\-\_：:（）()]+", " ", h).strip()
    for k, v in HEADER_SYNONYMS.items():
        if h_norm == re.sub(r"[\s\-\_：:（）()]+", " ", k.lower()).strip():
            return v
    for k, v in HEADER_SYNONYMS.items():
        if k.lower() in h or h in k.lower():
            return v
    return None

def detect_header_from_preview(df_preview: pd.DataFrame, max_header_rows=2, max_search_rows=8):
    if df_preview is None or df_preview.shape[0] == 0:
        return None, None
    nrows, ncols = df_preview.shape
    search_rows = min(max_search_rows, nrows)
    best = {"score": -1}
    for start in range(search_rows):
        for rows_used in range(1, max_header_rows + 1):
            if start + rows_used > nrows:
                continue
            cand = []
            nonempty = 0
            mapped = 0
            for col in range(ncols):
                parts = []
                for r in range(start, start + rows_used):
                    cell = df_preview.iat[r, col]
                    if pd.isna(cell):
                        continue
                    s = str(cell).strip()
                    if s:
                        parts.append(s)
                header_text = " ".join(parts).strip()
                if header_text:
                    nonempty += 1
                cand.append(header_text)
                if header_text and auto_map_header(header_text):
                    mapped += 1
            score = mapped + 0.5 * nonempty
            if score > best["score"]:
                best = {"score": score, "header": cand, "row": start, "rows_used": rows_used, "mapped": mapped, "nonempty": nonempty}
    if best.get("header") is not None:
        if best["mapped"] >= 2 or (best["nonempty"] > 0 and (best["mapped"] / best["nonempty"]) >= 0.3):
            return best["header"], best["row"] + best["rows_used"] - 1
    return None, None

def normalize_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    df_disp = df.copy()
    for col in df_disp.columns:
        try:
            ser = df_disp[col]
            if isinstance(ser, pd.DataFrame):
                df_disp[col] = ser.astype(str).apply(lambda x: x.str.slice(0, 100)).astype(str)
                continue
            if ser.dtype == "object":
                non_null = ser.dropna()
                if non_null.empty:
                    df_disp[col] = ser.where(ser.notna(), "").astype(str)
                    continue
                types_seen = {type(x) for x in non_null}
                has_bytes = any(isinstance(x, (bytes, bytearray, memoryview)) for x in non_null)
                multiple_types = len(types_seen) > 1
                if has_bytes or multiple_types:
                    df_disp[col] = ser.where(ser.notna(), None).apply(lambda x: "" if x is None else str(x))
                else:
                    df_disp[col] = ser.where(ser.notna(), None).apply(lambda x: "" if x is None else x)
        except Exception:
            df_disp[col] = df_disp[col].where(df_disp[col].notna(), None).apply(lambda x: "" if x is None else str(x))
    return df_disp

def safe_st_dataframe(df: pd.DataFrame, height: int | None = None):
    df_disp = normalize_for_display(df)
    try:
        if height is None:
            st.dataframe(df_disp)
        else:
            st.dataframe(df_disp, height=height)
    except Exception:
        df2 = df_disp.copy()
        for col in df2.columns:
            df2[col] = df2[col].astype(str).fillna("")
        if height is None:
            st.dataframe(df2)
        else:
            st.dataframe(df2, height=height)

# ============ Auth UI ============
def login_form():
    st.subheader("🔐 用户登录")
    u = st.text_input("用户名", key="login_user")
    p = st.text_input("密码", type="password", key="login_pass")
    if st.button("登录", key="login_button"):
        if not u or not p:
            st.error("请输入用户名和密码")
            return
        pw_hash = hashlib.sha256(p.encode()).hexdigest()
        with engine.begin() as conn:
            user = conn.execute(text("SELECT username, role, region FROM users WHERE username=:u AND password=:p"),
                                {"u": u, "p": pw_hash}).fetchone()
        if user:
            st.session_state["user"] = {"username": user.username, "role": user.role, "region": user.region}
            st.success(f"登录成功：{user.username}")
            safe_rerun()
        else:
            st.error("用户名或密码错误")

def register_form():
    st.subheader("🧾 注册")
    ru = st.text_input("新用户名", key="reg_user")
    rp = st.text_input("新密码", type="password", key="reg_pass")
    region = st.selectbox("地区", ["Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others"])
    if st.button("注册", key="reg_button"):
        if not ru or not rp:
            st.warning("用户名和密码不能为空")
        else:
            pw_hash = hashlib.sha256(rp.encode()).hexdigest()
            try:
                with engine.begin() as conn:
                    conn.execute(text("INSERT INTO users (username,password,role,region) VALUES (:u,:p,'user',:r)"),
                                 {"u": ru, "p": pw_hash, "r": region})
                st.success("注册成功，请登录")
            except Exception:
                st.error("用户名已存在")

def logout():
    if "user" in st.session_state:
        del st.session_state["user"]
    safe_rerun()

# ============ Page flow ============
if "user" not in st.session_state:
    tabs = st.tabs(["🔑 登录","🧾 注册"])
    with tabs[0]:
        login_form()
    with tabs[1]:
        register_form()
    st.stop()

if st.session_state.get("_needs_refresh", False):
    if st.button("手动刷新页面", key="manual_refresh"):
        safe_rerun()

user = st.session_state["user"]
st.sidebar.markdown(f"👤 **{user['username']}**  \n🏢 地区：{user['region']}  \n🔑 角色：{user['role']}")
if st.sidebar.button("退出登录", key="logout_btn"):
    logout()

page = st.sidebar.radio("导航", ["🏠 主页面", "📋 设备查询", "💰 杂费查询", "👑 管理员后台"] if user["role"]=="admin" else ["🏠 主页面", "📋 设备查询", "💰 杂费查询"])

# ============ Main: Upload / Mapping / Import ============
if page == "🏠 主页面":
    st.title("📊 询价录入与查询平台")
    st.header("📂 Excel 批量录入（智能表头映射）")
    st.caption("系统会尝试识别上传文件的表头并给出建议映射。")

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人","地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template.to_excel(writer, index=False)
    buf.seek(0)
    st.download_button("下载模板", buf, "quotation_template.xlsx", key="download_template")

    uploaded = st.file_uploader("上传 Excel (.xlsx)", type=["xlsx"], key="upload_excel")
    if uploaded:
        # ensure session flags
        if "mapping_done" not in st.session_state:
            st.session_state["mapping_done"] = False
        if "bulk_applied" not in st.session_state:
            st.session_state["bulk_applied"] = False

        try:
            preview = pd.read_excel(uploaded, header=None, nrows=50, dtype=object)
            safe_st_dataframe(preview.head(10))
        except Exception as e:
            st.error(f"读取预览失败：{e}")
            preview = None

        if preview is not None:
            header_names, header_row_index = detect_header_from_preview(preview, max_header_rows=2, max_search_rows=8)
            raw_df_full = pd.read_excel(uploaded, header=None, dtype=object)
            if header_names is None:
                header_row_index = 0
                header_names = [str(x) if not pd.isna(x) else "" for x in raw_df_full.iloc[0].tolist()]
            data_df = raw_df_full.iloc[header_row_index+1 : ].copy().reset_index(drop=True)
            if len(header_names) < data_df.shape[1]:
                header_names += [f"Unnamed_{i}" for i in range(len(header_names), data_df.shape[1])]
            elif len(header_names) > data_df.shape[1]:
                header_names = header_names[:data_df.shape[1]]

            data_df.columns = header_names

            st.markdown("**检测到的原始表头（用于映射，系统已尝试自动对应一版建议）：**")
            st.write(list(data_df.columns))

            mapping_targets = ["Ignore"] + [c for c in DB_COLUMNS if c not in ("录入人","地区")]

            auto_defaults = {}
            for col in data_df.columns:
                auto_val = auto_map_header(col)
                if auto_val and auto_val in mapping_targets:
                    auto_defaults[col] = auto_val
                else:
                    auto_defaults[col] = "Ignore"

            st.markdown("系统已为每一列生成建议映射（你可以直接点击“应用映射并预览” 或 修改任意下拉再提交）。")

            mapped_choices = {}
            with st.form("mapping_form", clear_on_submit=False):
                cols_left, cols_right = st.columns(2)
                for i, col in enumerate(data_df.columns):
                    default = auto_defaults.get(col, "Ignore")
                    container = cols_left if i % 2 == 0 else cols_right
                    sel = container.selectbox(f"源列: {col}", mapping_targets,
                                              index = mapping_targets.index(default) if default in mapping_targets else 0,
                                              key=f"map_{i}")
                    mapped_choices[col] = sel
                submitted = st.form_submit_button("应用映射并预览")

            if submitted:
                target_sources = {}
                for src, tgt in mapped_choices.items():
                    if tgt != "Ignore":
                        target_sources.setdefault(tgt, []).append(src)

                # robust mapped_but_empty detection
                mapped_but_empty = []
                for tgt, srcs in target_sources.items():
                    has_value = False
                    src_list = []
                    for item in srcs:
                        if isinstance(item, (list, tuple, set)):
                            src_list.extend(item)
                        else:
                            src_list.append(item)
                    for src_col in src_list:
                        if src_col in data_df.columns:
                            col_obj = data_df[src_col]
                            if isinstance(col_obj, pd.DataFrame):
                                try:
                                    ser = col_obj.fillna("").astype(str).agg(" ".join, axis=1)
                                except Exception:
                                    ser = col_obj.iloc[:, 0].astype(object)
                            else:
                                ser = col_obj.astype(object)

                            def normalize_val(x):
                                if pd.isna(x):
                                    return pd.NA
                                sx = str(x).strip()
                                if sx.lower() in ("", "nan", "none"):
                                    return pd.NA
                                return sx

                            try:
                                ser_norm = ser.map(normalize_val)
                            except Exception:
                                ser_norm = ser.astype(str).map(lambda x: None if x is None else (str(x).strip() if str(x).strip().lower() not in ("", "nan", "none") else pd.NA))

                            if ser_norm.notna().any():
                                has_value = True
                                break
                    if not has_value:
                        mapped_but_empty.append(tgt)

                # build df_for_db
                rename_dict = {orig: mapped for orig, mapped in mapped_choices.items() if mapped != "Ignore"}
                df_mapped = data_df.rename(columns=rename_dict).copy()
                for c in DB_COLUMNS:
                    if c not in df_mapped.columns:
                        df_mapped[c] = pd.NA
                df_mapped["录入人"] = user["username"]
                df_mapped["地区"] = user["region"]
                df_for_db = df_mapped[DB_COLUMNS]

                # save mapping to session
                csv_buf = io.StringIO()
                df_for_db.to_csv(csv_buf, index=False)
                st.session_state["mapping_csv"] = csv_buf.getvalue()
                st.session_state["mapping_done"] = True
                st.session_state["mapping_rename_dict"] = rename_dict
                st.session_state["mapping_target_sources"] = target_sources
                st.session_state["mapping_mapped_but_empty"] = mapped_but_empty

                st.success("映射已保存。现在请填写全局必填信息并提交以继续校验与导入。")

    # ====== 映射后预览 + 更稳健的“填写全局信息并导入” 流程 ======
    mapping_csv = st.session_state.get("mapping_csv", None)
    if mapping_csv:
        try:
            csv_buf = io.StringIO(mapping_csv)
            df_for_db = pd.read_csv(csv_buf, dtype=object)
            for c in DB_COLUMNS:
                if c not in df_for_db.columns:
                    df_for_db[c] = pd.NA
            df_for_db = df_for_db[DB_COLUMNS]
        except Exception as e:
            st.error(f"恢复映射数据失败：{e}")
            df_for_db = None

        st.markdown("**映射后预览（前 10 行）：**")
        if df_for_db is not None:
            safe_st_dataframe(df_for_db.head(10))
        else:
            st.info("映射数据无法预览，请重新映射。")

        if "show_global_form" not in st.session_state:
            st.session_state["show_global_form"] = False

        col_show, col_hint = st.columns([1, 6])
        if col_show.button("➡️ 填写/查看全局信息并应用导入", key="open_global_form_btn"):
            st.session_state["show_global_form"] = True
        col_hint.markdown("（若需要对空值进行统一填充，例如币种/项目/供应商/询价人，请展开并填写全局信息）")

        if st.session_state["show_global_form"]:
            if "bulk_values" not in st.session_state:
                st.session_state["bulk_values"] = {"project": "", "supplier": "", "enquirer": "", "date": "", "currency": ""}

            def column_has_empty_currency(df: pd.DataFrame) -> bool:
                if df is None or "币种" not in df.columns:
                    return True
                ser = df["币种"]
                def is_empty_val(x):
                    if pd.isna(x):
                        return True
                    s = str(x).strip().lower()
                    return s == "" or s in ("nan", "none")
                return any(is_empty_val(x) for x in ser)

            need_global_currency = column_has_empty_currency(df_for_db)

            st.markdown("请填写全局必填信息（仅填充空值）。填写完后点击“应用全局并继续校验”：")
            with st.form("global_form_v2"):
                g1, g2, g3, g4, g5 = st.columns(5)
                g_project = g1.text_input("项目名称", key="global_project_input", value=st.session_state["bulk_values"].get("project", ""))
                g_supplier = g2.text_input("供应商名称", key="global_supplier_input", value=st.session_state["bulk_values"].get("supplier", ""))
                g_enquirer = g3.text_input("询价人", key="global_enquirer_input", value=st.session_state["bulk_values"].get("enquirer", ""))
                default_date = st.session_state["bulk_values"].get("date", "")
                try:
                    if default_date:
                        g_date = g4.date_input("询价日期", value=pd.to_datetime(default_date).date(), key="global_date_input")
                    else:
                        g_date = g4.date_input("询价日期", value=date.today(), key="global_date_input")
                except Exception:
                    g_date = g4.date_input("询价日期", value=date.today(), key="global_date_input")

                g_currency = None
                if need_global_currency:
                    currency_options = ["", "IDR", "USD", "RMB", "SGD", "MYR", "THB"]
                    curr_default = st.session_state["bulk_values"].get("currency", "")
                    default_idx = currency_options.index(curr_default) if curr_default in currency_options else 0
                    g_currency = g5.selectbox("币种（用于填充空值）", currency_options, index=default_idx, key="global_currency_input")
                else:
                    g5.write("")

                apply_global = st.form_submit_button("应用全局并继续校验")

            if apply_global:
                if not (g_project and g_supplier and g_enquirer and g_date):
                    st.error("必须填写：项目名称、供应商名称、询价人和询价日期")
                    st.session_state["bulk_applied"] = False
                elif need_global_currency and (g_currency is None or str(g_currency).strip() == ""):
                    st.error("由于源数据存在空的币种，请选择币种以继续。")
                    st.session_state["bulk_applied"] = False
                else:
                    st.session_state["bulk_values"] = {
                        "project": str(g_project),
                        "supplier": str(g_supplier),
                        "enquirer": str(g_enquirer),
                        "date": str(g_date),
                        "currency": str(g_currency) if g_currency is not None else st.session_state["bulk_values"].get("currency", "")
                    }
                    st.session_state["bulk_applied"] = True
                    st.success("已应用全局信息，正在进行总体必填校验...")

            if st.session_state.get("bulk_applied", False):
                try:
                    csv_buf2 = io.StringIO(st.session_state["mapping_csv"])
                    df_for_db = pd.read_csv(csv_buf2, dtype=object)
                    for c in DB_COLUMNS:
                        if c not in df_for_db.columns:
                            df_for_db[c] = pd.NA
                    df_for_db = df_for_db[DB_COLUMNS]
                except Exception as e:
                    st.error(f"恢复映射数据失败：{e}")
                    df_for_db = None

                if df_for_db is None:
                    st.error("映射数据丢失，无法继续导入。")
                else:
                    df_final = df_for_db.copy()
                    g = st.session_state["bulk_values"]

                    def is_empty_cell(x):
                        if pd.isna(x):
                            return True
                        sx = str(x).strip()
                        return sx == "" or sx.lower() in ("nan", "none")

                    def fill_empty(col_name, value):
                        if col_name not in df_final.columns:
                            df_final[col_name] = pd.NA
                        mask = df_final[col_name].apply(lambda x: is_empty_cell(x))
                        if mask.any():
                            df_final.loc[mask, col_name] = value

                    fill_empty("项目名称", str(g["project"]))
                    fill_empty("供应商名称", str(g["supplier"]))
                    fill_empty("询价人", str(g["enquirer"]))
                    fill_empty("询价日期", str(g["date"]))
                    if need_global_currency and g.get("currency"):
                        fill_empty("币种", str(g["currency"]))

                    # --- New validation: brand NOT required; price rule: either 设备单价 or 人工包干单价 must be present
                    def normalize_cell(x):
                        if pd.isna(x):
                            return None
                        s = str(x).strip()
                        if s.lower() in ("", "nan", "none"):
                            return None
                        return s

                    # required non-price fields (brand is NOT required)
                    required_nonprice = ["项目名称","供应商名称","询价人","设备材料名称","币种","询价日期"]
                    check_nonprice = df_final[required_nonprice].applymap(normalize_cell)
                    missing_nonprice = check_nonprice.isna().any(axis=1)

                    def price_has_value(row) -> bool:
                        v1 = row.get("设备单价", None) if "设备单价" in row.index else None
                        v2 = row.get("人工包干单价", None) if "人工包干单价" in row.index else None
                        nv1 = normalize_cell(v1)
                        nv2 = normalize_cell(v2)
                        return (nv1 is not None) or (nv2 is not None)

                    price_mask = df_final.apply(price_has_value, axis=1)
                    rows_invalid_mask = missing_nonprice | (~price_mask)

                    df_valid = df_final[~rows_invalid_mask].copy()
                    df_invalid = df_final[rows_invalid_mask].copy()

                    imported_count = 0
                    if not df_valid.empty:
                        try:
                            df_to_store = df_valid.dropna(how="all").drop_duplicates().reset_index(drop=True)
                            # final check on critical cols: device name and price/labor-price rule already ensured
                            final_check = df_to_store[["设备材料名称","设备单价","人工包干单价","币种"]].applymap(normalize_cell)
                            # ensure price/labor present
                            def final_price_ok(row):
                                v1 = row.get("设备单价", None)
                                v2 = row.get("人工包干单价", None)
                                return (v1 is not None) or (v2 is not None)
                            final_invalid_mask = final_check["设备材料名称"].isna() | (~df_to_store.apply(final_price_ok, axis=1))
                            if final_invalid_mask.any():
                                to_import = df_to_store[~final_invalid_mask].copy()
                                still_bad = df_to_store[final_invalid_mask].copy()
                                if not to_import.empty:
                                    with engine.begin() as conn:
                                        to_import.to_sql("quotations", conn, if_exists="append", index=False)
                                    imported_count = len(to_import)
                                    st.success(f"✅ 已导入 {imported_count} 条有效记录（跳过 {len(still_bad)} 条）。")
                                else:
                                    st.info("没有可导入的有效记录（所有候选在最终检查中被判为不完整）。")
                                if not still_bad.empty:
                                    df_invalid = pd.concat([df_invalid, still_bad], ignore_index=True)
                            else:
                                with engine.begin() as conn:
                                    df_to_store.to_sql("quotations", conn, if_exists="append", index=False)
                                imported_count = len(df_to_store)
                                st.success(f"✅ 已导入全部 {imported_count} 条有效记录。")
                        except Exception as e:
                            st.error(f"导入有效记录时发生错误：{e}")
                    else:
                        st.info("没有找到满足总体必填条件的记录可导入。")

                    if not df_invalid.empty:
                        st.warning(f"以下 {len(df_invalid)} 条记录缺少总体必填字段，未被导入，请修正后重新导入：")
                        safe_st_dataframe(df_invalid.head(50))
                        buf_bad = io.BytesIO()
                        with pd.ExcelWriter(buf_bad, engine="openpyxl") as w:
                            df_invalid.to_excel(w, index=False)
                        buf_bad.seek(0)
                        st.download_button("📥 下载未通过记录（用于修正）", buf_bad, "invalid_rows.xlsx")

                    st.session_state["bulk_applied"] = False
    else:
        st.info("映射保存。请填写全局信息（若必要）并应用以继续导入。")

    # ------------------ 手工录入（原始逻辑，已调整：品牌不再必填） ------------------
    st.header("✏️ 手工录入设备（原始逻辑）")
    with st.form("manual_add_form_original", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input("项目名称", key="manual_project_orig")
        sup = col2.text_input("供应商名称", key="manual_supplier_orig")
        inq = col3.text_input("询价人", key="manual_enquirer_orig")
        name = st.text_input("设备材料名称", key="manual_name_orig")
        brand = st.text_input("品牌（可选）", key="manual_brand_orig")
        qty = st.number_input("数量确认", min_value=0.0, key="manual_qty_orig")
        price = st.number_input("设备单价", min_value=0.0, key="manual_price_orig")
        labor_price = st.number_input("人工包干单价", min_value=0.0, key="manual_labor_price_orig")
        cur = st.selectbox("币种", ["IDR","USD","RMB","SGD","MYR","THB"], key="manual_currency_orig")
        desc = st.text_area("描述", key="manual_desc_orig")
        date_inq = st.date_input("询价日期", value=date.today(), key="manual_date_orig")
        submit_manual = st.form_submit_button("添加记录（手动）", key="manual_submit_orig")

    if submit_manual:
        # validate required fields except brand
        if not (pj and sup and inq and name):
            st.error("必填项不能为空：项目名称、供应商名称、询价人、设备材料名称")
        else:
            # price rule: either price or labor_price must be provided
            def has_price_value(v):
                if pd.isna(v):
                    return False
                s = str(v).strip()
                if s == "" or s.lower() in ("nan","none"):
                    return False
                return True
            if not (has_price_value(price) or has_price_value(labor_price)):
                st.error("请至少填写 设备单价 或 人工包干单价 中的一项（两者至少填一项）。")
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO quotations (项目名称,供应商名称,询价人,设备材料名称,品牌,数量确认,设备单价,人工包干单价,币种,描述,录入人,地区,询价日期)
                            VALUES (:p,:s,:i,:n,:b,:q,:pr,:lp,:c,:d,:u,:reg,:dt)
                        """), {"p": pj, "s": sup, "i": inq, "n": name, "b": brand if brand is not None else "",
                               "q": qty, "pr": price if price != 0 else None,
                               "lp": labor_price if labor_price != 0 else None,
                               "c": cur, "d": desc, "u": user["username"], "reg": user["region"], "dt": str(date_inq)})
                    st.success("手工记录已添加（按原逻辑，品牌为可选）。")
                except Exception as e:
                    st.error(f"添加记录失败：{e}")

# ============ Search / Delete (Admin) ============
if page == "📋 设备查询":
    st.header("📋 设备查询")
    kw = st.text_input("关键词（多个空格分词）", key="search_kw")
    search_fields = st.multiselect("搜索字段（留空为默认）",
                                   ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称", "地区"],
                                   key="search_fields")
    pj_filter = st.text_input("按项目名称过滤", key="search_pj")
    sup_filter = st.text_input("按供应商名称过滤", key="search_sup")
    brand_filter = st.text_input("按品牌过滤", key="search_brand")
    cur_filter = st.selectbox("币种", ["全部","IDR","USD","RMB","SGD","MYR","THB"], index=0, key="search_cur")

    regions_options = ["全部","Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others","All"]
    if user["role"] == "admin":
        region_filter = st.selectbox("按地区过滤（管理员）", regions_options, index=0, key="search_region")
    else:
        st.info(f"仅显示您所在地区的数据：{user['region']}")
        region_filter = user["region"]

    if st.button("🔍 搜索设备", key="search_button"):
        conds = []
        params = {}
        if pj_filter:
            conds.append("LOWER(项目名称) LIKE :pj")
            params["pj"] = f"%{pj_filter.lower()}%"
        if sup_filter:
            conds.append("LOWER(供应商名称) LIKE :sup")
            params["sup"] = f"%{sup_filter.lower()}%"
        if brand_filter:
            conds.append("LOWER(品牌) LIKE :brand")
            params["brand"] = f"%{brand_filter.lower()}%"
        if cur_filter != "全部":
            conds.append("币种 = :cur")
            params["cur"] = cur_filter
        if user["role"] != "admin":
            conds.append("地区 = :r")
            params["r"] = user["region"]
        else:
            if region_filter and region_filter != "全部":
                conds.append("地区 = :r")
                params["r"] = region_filter

        if kw:
            tokens = re.findall(r"\S+", kw)
            fields = search_fields if search_fields else ["设备材料名称","描述","品牌","规格或型号","项目名称","供应商名称"]
            for i, t in enumerate(tokens):
                ors = []
                for j, f in enumerate(fields):
                    pname = f"kw_{i}_{j}"
                    ors.append(f"LOWER({f}) LIKE :{pname}")
                    params[pname] = f"%{t.lower()}%"
                conds.append("(" + " OR ".join(ors) + ")")

        sql = "SELECT rowid, * FROM quotations"
        if conds:
            sql += " WHERE " + " AND ".join(conds)

        try:
            df = pd.read_sql(sql, engine, params=params)
        except Exception as e:
            st.error(f"查询失败：{e}")
            df = pd.DataFrame()

        if df.empty:
            st.info("未找到符合条件的记录。")
        else:
            safe_st_dataframe(df)
            # download
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            buf.seek(0)
            st.download_button("下载结果", buf, "设备查询结果.xlsx", key="download_search")

            # Admin delete form (single form)
            if user["role"] == "admin":
                st.markdown("---")
                st.markdown("⚠️ 管理员删除：选择记录并确认。")
                choices = []
                for _, row in df.iterrows():
                    rid = int(row["rowid"])
                    proj = str(row.get("项目名称",""))[:40]
                    name = str(row.get("设备材料名称",""))[:60]
                    brand = str(row.get("品牌",""))[:30]
                    choices.append(f"{rid} | {proj} | {name} | {brand}")

                with st.form("admin_delete_form_final_v2", clear_on_submit=False):
                    selected = st.multiselect("选中要删除的记录", choices, key="admin_delete_selected_v2")
                    confirm = st.checkbox("我确认删除所选记录（不可恢复）", key="admin_delete_confirm_v2")
                    submit_del = st.form_submit_button("删除所选记录（管理员）", key="admin_delete_submit_v2")

                if submit_del:
                    if not selected:
                        st.warning("请先选择要删除的记录。")
                    elif not confirm:
                        st.warning("请勾选确认框以执行删除。")
                    else:
                        try:
                            selected_rowids = [int(s.split("|",1)[0].strip()) for s in selected]
                        except Exception as e:
                            st.error(f"解析所选 rowid 失败：{e}")
                            selected_rowids = []

                        if not selected_rowids:
                            st.warning("无有效 rowid，取消删除。")
                        else:
                            placeholders = ",".join(str(int(r)) for r in selected_rowids)
                            select_verify_sql = f"SELECT rowid, 项目名称, 供应商名称, 设备材料名称, 品牌 FROM quotations WHERE rowid IN ({placeholders})"
                            try:
                                matched_df = pd.read_sql(select_verify_sql, engine)
                            except Exception as e:
                                st.error(f"匹配查询失败：{e}")
                                matched_df = pd.DataFrame()

                            if matched_df.empty:
                                st.warning("未在数据库中匹配到所选 rowid，取消删除。")
                                st.write("执行的 SELECT SQL：", select_verify_sql)
                            else:
                                st.markdown("以下为将被删除的匹配记录，请核对：")
                                safe_st_dataframe(matched_df)

                                # Try archive first (ignore archive errors)
                                try:
                                    with engine.begin() as conn:
                                        conn.execute(text(f"""
                                            INSERT INTO deleted_quotations
                                            SELECT rowid AS original_rowid, 序号, 设备材料名称, 规格或型号, 描述, 品牌, 单位, 数量确认,
                                                   报价品牌, 型号, 设备单价, 设备小计, 人工包干单价, 人工包干小计, 综合单价汇总,
                                                   币种, 原厂品牌维保期限, 货期, 备注, 询价人, 项目名称, 供应商名称, 询价日期, 录入人, 地区,
                                                   CURRENT_TIMESTAMP AS deleted_at, :user AS deleted_by
                                            FROM quotations WHERE rowid IN ({placeholders})
                                        """), {"user": user["username"]})
                                    st.write("已尝试归档（若表不存在则忽略）。")
                                except Exception as e_arch:
                                    st.warning(f"归档异常（已忽略）：{e_arch}")

                                # Execute DELETE and check rowcount
                                delete_sql = f"DELETE FROM quotations WHERE rowid IN ({placeholders})"
                                try:
                                    with engine.begin() as conn:
                                        res = conn.execute(text(delete_sql))
                                        deleted_count = getattr(res, "rowcount", None)
                                    if deleted_count is None:
                                        st.info("删除执行，但未获取 rowcount，请查询确认。")
                                    elif deleted_count == 0:
                                        st.warning("DELETE 执行成功但未删除任何行（rowcount=0）。")
                                    else:
                                        st.success(f"已删除 {deleted_count} 条记录。")
                                except Exception as e_del:
                                    st.error(f"执行 DELETE 时异常：{e_del}")

                                # Verify after deletion
                                try:
                                    after_df = pd.read_sql(select_verify_sql, engine)
                                    if after_df.empty:
                                        st.info("删除后复查未找到这些记录（删除成功）。")
                                    else:
                                        st.warning("删除后仍查询到部分记录（请检查）：")
                                        safe_st_dataframe(after_df)
                                except Exception as e_after:
                                    st.warning(f"删除后复核失败：{e_after}")

                                safe_rerun()
            else:
                st.info("仅管理员可删除记录。")

# ============ Misc costs page ============
elif page == "💰 杂费查询":
    st.header("💰 杂费查询")
    pj2 = st.text_input("按项目名称过滤", key="misc_pj")
    if st.button("🔍 搜索杂费", key="misc_search"):
        params = {"pj": f"%{pj2.lower()}%"}
        sql = "SELECT * FROM misc_costs WHERE LOWER(项目名称) LIKE :pj"
        if user["role"] != "admin":
            sql += " AND 地区 = :r"
            params["r"] = user["region"]
        df2 = pd.read_sql(sql, engine, params=params)
        safe_st_dataframe(df2)
        if not df2.empty:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as writer:
                df2.to_excel(writer, index=False)
            buf2.seek(0)
            st.download_button("下载杂费结果", buf2, "misc_costs.xlsx", key="download_misc")

# ============ Admin page ============
elif page == "👑 管理员后台" and user["role"] == "admin":
    st.header("👑 管理后台")
    users_df = pd.read_sql("SELECT username, role, region FROM users", engine)
    safe_st_dataframe(users_df)
