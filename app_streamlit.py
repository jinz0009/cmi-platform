# -*- coding: utf-8 -*-
"""
Complete app_streamlit.py with compatibility-safe rerun and previous fixes integrated.

Changes:
- Added safe_rerun() to replace direct calls to st.experimental_rerun() for compatibility.
- Replaced all st.experimental_rerun() calls with safe_rerun().
- Retained previous robustness fixes:
  - normalize_for_display + safe_st_dataframe to avoid pyarrow ArrowTypeError.
  - Robust mapped_but_empty detection and fill-only-empty global apply.
  - Admin delete verified with SELECT and using result.rowcount.
- Unique widget keys to avoid session_state collisions.

Usage:
- Save as main/app_streamlit.py and run: streamlit run main/app_streamlit.py
- Default admin user: admin / admin123 (example only).
"""
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib, io, re
from datetime import date

st.set_page_config(page_title="CMI 询价录入与查询平台", layout="wide")

# --- Compatibility helper: safe_rerun ---
def safe_rerun():
    """
    Attempt to perform a Streamlit rerun in a way that works across versions.
    - Prefer st.experimental_rerun if available.
    - Otherwise, try raising internal RerunException if available.
    - As a last resort set a session flag and show a warning to the user.
    """
    try:
        if hasattr(st, "experimental_rerun"):
            st.experimental_rerun()
            return
        # Try internal exception (different streamlit versions hide this)
        try:
            # This import may fail on some versions
            from streamlit.runtime.scriptrunner import RerunException
            raise RerunException()
        except Exception:
            # Final fallback
            st.session_state["_needs_refresh"] = True
            st.warning("请手动刷新页面以查看最新状态（自动刷新在当前 Streamlit 版本不可用）。")
            return
    except Exception:
        st.session_state["_needs_refresh"] = True
        st.warning("无法自动重启，请手动刷新浏览器页面。")
        return

# Database engine (adjust URI in production)
engine = create_engine("sqlite:///quotation.db", connect_args={"check_same_thread": False})

# ============ Initialize DB ============
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
        品牌 TEXT NOT NULL,
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

def normalize_for_display(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame so Streamlit/pyarrow can serialize it safely."""
    if df is None:
        return df
    df_disp = df.copy()
    for col in df_disp.columns:
        try:
            ser = df_disp[col]
            # If it's DataFrame-like accidentally, coerce to str
            if isinstance(ser, pd.DataFrame):
                df_disp[col] = ser.astype(str).apply(lambda x: x.str.slice(0, 100)).astype(str)
                continue
            # For object dtypes, ensure consistent element types (stringify mixed)
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
        # Last resort stringify everything
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
    username = st.text_input("用户名", key="login_user")
    password = st.text_input("密码", type="password", key="login_pass")
    if st.button("登录", key="login_button"):
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        with engine.begin() as conn:
            user = conn.execute(text("SELECT * FROM users WHERE username=:u AND password=:p"), {"u": username, "p": pw_hash}).fetchone()
        if user:
            st.session_state["user"] = {"username": username, "role": user.role, "region": user.region}
            st.success(f"✅ 登录成功！欢迎 {username}（{user.region}）")
            safe_rerun()
        else:
            st.error("❌ 用户名或密码错误")

def register_form():
    st.subheader("🧾 注册")
    ru = st.text_input("新用户名", key="reg_user")
    rp = st.text_input("新密码", type="password", key="reg_pass")
    region = st.selectbox("地区", ["Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others"], key="reg_region")
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

# If earlier safe_rerun set a refresh flag, show a manual refresh button
if st.session_state.get("_needs_refresh", False):
    if st.button("手动刷新页面", key="manual_refresh"):
        # try best-effort rerun
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

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人","地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template.to_excel(writer, index=False)
    buf.seek(0)
    st.download_button("下载模板", buf, "quotation_template.xlsx", key="download_template")

    uploaded = st.file_uploader("上传 Excel (.xlsx)", type=["xlsx"], key="upload_excel")
    if uploaded:
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
            data_df = raw_df_full.iloc[header_row_index+1:].copy().reset_index(drop=True)
            if len(header_names) < data_df.shape[1]:
                header_names += [f"Unnamed_{i}" for i in range(len(header_names), data_df.shape[1])]
            data_df.columns = header_names

            st.write("原始表头：", list(data_df.columns))

            mapping_targets = ["Ignore"] + [c for c in DB_COLUMNS if c not in ("录入人","地区")]
            auto_defaults = {col: (auto_map_header(col) if auto_map_header(col) in mapping_targets else "Ignore") for col in data_df.columns}

            with st.form("mapping_form_full", clear_on_submit=False):
                cols_l, cols_r = st.columns(2)
                mapped = {}
                for i, col in enumerate(data_df.columns):
                    container = cols_l if i % 2 == 0 else cols_r
                    default = auto_defaults.get(col, "Ignore")
                    sel = container.selectbox(f"{col}", mapping_targets, index=mapping_targets.index(default) if default in mapping_targets else 0, key=f"map_full_{i}")
                    mapped[col] = sel
                submit_map = st.form_submit_button("应用映射并预览")

            if submit_map:
                # Build target_sources: target -> [src1, src2,...]
                target_sources = {}
                for src_col, tgt in mapped.items():
                    if tgt != "Ignore":
                        target_sources.setdefault(tgt, []).append(src_col)

                # Robust mapped_but_empty detection
                mapped_but_empty = []
                for tgt, srcs in target_sources.items():
                    has_value = False
                    # flatten any nested lists defensively
                    src_list = []
                    for item in srcs:
                        if isinstance(item, (list, tuple, set)):
                            src_list.extend(item)
                        else:
                            src_list.append(item)
                    for src_col in src_list:
                        if src_col in data_df.columns:
                            ser = data_df[src_col].astype(object)
                            try:
                                ser_norm = ser.where(~ser.astype(str).str.strip().isin(["", "nan", "none"]), pd.NA)
                            except Exception:
                                ser_norm = ser.apply(lambda x: None if pd.isna(x) else (str(x).strip() if str(x).strip().lower() not in ("", "nan", "none") else pd.NA))
                            if ser_norm.dropna().shape[0] > 0:
                                has_value = True
                                break
                    if not has_value:
                        mapped_but_empty.append(tgt)

                # Build df_for_db
                rename_dict = {k: v for k, v in mapped.items() if v != "Ignore"}
                df_mapped = data_df.rename(columns=rename_dict)
                for c in DB_COLUMNS:
                    if c not in df_mapped.columns:
                        df_mapped[c] = pd.NA
                df_mapped["录入人"] = user["username"]
                df_mapped["地区"] = user["region"]
                df_for_db = df_mapped[DB_COLUMNS]

                # Save mapping CSV to session
                csv_buf = io.StringIO()
                df_for_db.to_csv(csv_buf, index=False)
                st.session_state["mapping_csv"] = csv_buf.getvalue()
                st.session_state["mapping_done"] = True
                st.session_state["mapping_target_sources"] = target_sources
                st.session_state["mapping_mapped_but_empty"] = mapped_but_empty

                st.success("映射保存。请填写全局信息（若必要）并应用以继续导入。")
                if mapped_but_empty:
                    st.warning(f"注意：以下目标列从源数据未检测到有效值：{', '.join(mapped_but_empty)}")

    # Manual entry form
    st.header("✏️ 手工录入设备")
    with st.form("manual_add_form", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input("项目名称", key="manual_pj")
        sup = col2.text_input("供应商名称", key="manual_sup")
        inq = col3.text_input("询价人", key="manual_inq")
        name = st.text_input("设备材料名称", key="manual_name")
        brand = st.text_input("品牌", key="manual_brand")
        qty = st.number_input("数量确认", min_value=0.0, key="manual_qty")
        price = st.number_input("设备单价", min_value=0.0, key="manual_price")
        cur = st.selectbox("币种", ["IDR","USD","RMB","SGD","MYR","THB"], key="manual_cur")
        desc = st.text_area("描述", key="manual_desc")
        date_inq = st.date_input("询价日期", key="manual_date")
        submit_manual = st.form_submit_button("添加记录")
    if submit_manual:
        if not (pj and sup and inq and name and brand):
            st.error("必填项不能为空")
        else:
            with engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO quotations (项目名称,供应商名称,询价人,设备材料名称,品牌,数量确认,设备单价,币种,描述,录入人,地区,询价日期)
                    VALUES (:p,:s,:i,:n,:b,:q,:pr,:c,:d,:u,:reg,:dt)
                """), {"p": pj, "s": sup, "i": inq, "n": name, "b": brand, "q": qty, "pr": price,
                       "c": cur, "d": desc, "u": user["username"], "reg": user["region"], "dt": str(date_inq)})
            st.success("手工记录已添加")

    # Apply global values and import if mapping exists in session
    if st.session_state.get("mapping_done", False) and st.session_state.get("mapping_csv", None):
        st.markdown("---")
        st.markdown("请填写全局信息（会填充到映射表中的缺失项，仅填空处）：")
        # load df_for_db
        csv_buf = io.StringIO(st.session_state["mapping_csv"])
        df_for_db = pd.read_csv(csv_buf, dtype=object)
        for c in DB_COLUMNS:
            if c not in df_for_db.columns:
                df_for_db[c] = pd.NA
        df_for_db = df_for_db[DB_COLUMNS]

        # show preview safely
        st.markdown("映射后预览（前10行）：")
        safe_st_dataframe(df_for_db.head(10))

        with st.form("global_form_apply", clear_on_submit=False):
            col_a, col_b, col_c, col_d, col_e = st.columns(5)
            global_project = col_a.text_input("项目名称", value=st.session_state.get("global_project", ""), key="global_project")
            global_supplier = col_b.text_input("供应商名称", value=st.session_state.get("global_supplier", ""), key="global_supplier")
            global_enquirer = col_c.text_input("询价人", value=st.session_state.get("global_enquirer", ""), key="global_enquirer")
            default_date = st.session_state.get("global_date", "")
            try:
                if default_date:
                    global_date = col_d.date_input("询价日期", value=pd.to_datetime(default_date).date(), key="global_date")
                else:
                    global_date = col_d.date_input("询价日期", value=date.today(), key="global_date")
            except Exception:
                global_date = col_d.date_input("询价日期", value=date.today(), key="global_date")
            global_currency = col_e.selectbox("币种（用于填充空值）", ["","IDR","USD","RMB","SGD","MYR","THB"], index=0, key="global_currency")
            apply_global = st.form_submit_button("应用全局并继续导入")
        if apply_global:
            # basic required checks
            if not (global_project and global_supplier and global_enquirer and global_date):
                st.error("必须填写：项目名称、供应商名称、询价人和询价日期")
            else:
                # fill only empty values (do not overwrite existing)
                df_final = df_for_db.copy()
                df_final["项目名称"] = df_final["项目名称"].fillna("").astype(str)
                mask_proj = df_final["项目名称"].astype(str).str.strip() == ""
                df_final.loc[mask_proj, "项目名称"] = str(global_project)

                df_final["供应商名称"] = df_final["供应商名称"].fillna("").astype(str)
                mask_sup = df_final["供应商名称"].astype(str).str.strip() == ""
                df_final.loc[mask_sup, "供应商名称"] = str(global_supplier)

                df_final["询价人"] = df_final["询价人"].fillna("").astype(str)
                mask_inq = df_final["询价人"].astype(str).str.strip() == ""
                df_final.loc[mask_inq, "询价人"] = str(global_enquirer)

                df_final["询价日期"] = df_final["询价日期"].fillna("").astype(str)
                mask_date = df_final["询价日期"].astype(str).str.strip() == ""
                df_final.loc[mask_date, "询价日期"] = str(global_date)

                if global_currency:
                    df_final["币种"] = df_final["币种"].fillna("").astype(str)
                    mask_cur = df_final["币种"].astype(str).str.strip() == ""
                    df_final.loc[mask_cur, "币种"] = str(global_currency)

                # Normalize empties and check overall required
                def normalize_cell(x):
                    if pd.isna(x):
                        return None
                    s = str(x).strip()
                    if s.lower() in ("", "nan", "none"):
                        return None
                    return s

                overall_required = ["项目名称","供应商名称","询价人","设备材料名称","品牌","设备单价","币种","询价日期"]
                check_df = df_final[overall_required].applymap(normalize_cell)
                rows_missing_mask = check_df.isna().any(axis=1)

                df_valid = df_final[~rows_missing_mask].copy()
                df_invalid = df_final[rows_missing_mask].copy()

                imported_count = 0
                if not df_valid.empty:
                    try:
                        df_to_store = df_valid.dropna(how="all").drop_duplicates().reset_index(drop=True)
                        # final insert
                        with engine.begin() as conn:
                            df_to_store.to_sql("quotations", conn, if_exists="append", index=False)
                        imported_count = len(df_to_store)
                        st.success(f"已导入 {imported_count} 条记录")
                    except Exception as e:
                        st.error(f"导入异常：{e}")
                else:
                    st.info("没有满足必填条件的记录可导入")

                if not df_invalid.empty:
                    st.warning(f"{len(df_invalid)} 条记录缺少必填字段，已显示供您下载修正")
                    safe_st_dataframe(df_invalid.head(50))
                    buf_bad = io.BytesIO()
                    with pd.ExcelWriter(buf_bad, engine="openpyxl") as w:
                        df_invalid.to_excel(w, index=False)
                    buf_bad.seek(0)
                    st.download_button("下载未通过记录", buf_bad, "invalid_rows.xlsx", key="download_invalid")
                # clear mapping session to avoid reapply accidentally
                if imported_count > 0:
                    st.session_state.pop("mapping_csv", None)
                    st.session_state.pop("mapping_done", None)
                    st.session_state.pop("mapping_target_sources", None)
                    st.session_state.pop("mapping_mapped_but_empty", None)

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
