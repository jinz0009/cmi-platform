# -*- coding: utf-8 -*-
"""
Streamlit App (Postgres / Neon version) — UI refresh (Wix-like tech style)
- Keeps ALL functional modules unchanged (login/register/upload/import/search/misc/admin).
- Only changes UI layout + CSS: top nav, dark tech theme, cards, nicer tables & buttons.
Run:
    streamlit run app_streamlit.py
Streamlit Cloud:
    Secrets:
        DB_URL="postgresql+psycopg2://USER:PASSWORD@HOST/DB?sslmode=require"
"""

import os
import re
import io
import hashlib
from datetime import date

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool


# ==================== PAGE CONFIG ====================
st.set_page_config(page_title="CMI 询价录入与查询平台", layout="wide")


# ==================== THEME (Wix-like Tech Dark) ====================
THEME_CSS = """
/* Base */
:root{
  --bg0: #070A12;
  --bg1: #0B1020;
  --card: rgba(255,255,255,0.06);
  --card2: rgba(255,255,255,0.08);
  --line: rgba(255,255,255,0.12);
  --text: rgba(255,255,255,0.92);
  --muted: rgba(255,255,255,0.64);
  --muted2: rgba(255,255,255,0.52);
  --brand: #5B8CFF;
  --brand2:#7C4DFF;
  --good:#35D07F;
  --warn:#FFB020;
  --bad:#FF5A65;
  --shadow: 0 12px 40px rgba(0,0,0,0.45);
  --r: 18px;
}

html, body, [data-testid="stAppViewContainer"]{
  background: radial-gradient(1100px 700px at 10% 10%, rgba(124,77,255,0.18), transparent 60%),
              radial-gradient(900px 600px at 80% 20%, rgba(91,140,255,0.20), transparent 55%),
              linear-gradient(180deg, var(--bg0) 0%, var(--bg1) 100%) !important;
  color: var(--text) !important;
}

[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stToolbar"] { right: 1rem; }

/* Make main wider and cleaner */
.block-container{
  padding-top: 1.2rem !important;
  padding-bottom: 2.2rem !important;
}

/* Sidebar: keep available but subtle */
[data-testid="stSidebar"]{
  background: rgba(255,255,255,0.03) !important;
  border-right: 1px solid var(--line) !important;
}
[data-testid="stSidebar"] * { color: var(--text) !important; }

/* Typography */
h1,h2,h3 { letter-spacing: 0.2px; }
h1 { font-size: 2.0rem !important; }
h2 { font-size: 1.35rem !important; }
p, label, .stCaption { color: var(--muted) !important; }

/* Buttons */
.stButton > button, .stDownloadButton > button{
  border-radius: 999px !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  background: linear-gradient(90deg, rgba(91,140,255,0.30), rgba(124,77,255,0.22)) !important;
  color: var(--text) !important;
  box-shadow: 0 10px 26px rgba(0,0,0,0.35);
  padding: 0.55rem 0.95rem !important;
}
.stButton > button:hover, .stDownloadButton > button:hover{
  transform: translateY(-1px);
  border-color: rgba(255,255,255,0.25) !important;
  filter: brightness(1.05);
}
.stButton > button:active, .stDownloadButton > button:active{
  transform: translateY(0px);
  filter: brightness(0.98);
}

/* Inputs */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input,
[data-testid="stDateInput"] input,
[data-testid="stTextArea"] textarea{
  background: rgba(255,255,255,0.06) !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  border-radius: 12px !important;
  color: var(--text) !important;
}
[data-testid="stSelectbox"] div[role="combobox"]{
  background: rgba(255,255,255,0.06) !important;
  border: 1px solid rgba(255,255,255,0.14) !important;
  border-radius: 12px !important;
  color: var(--text) !important;
}

/* Tabs (top nav feel) */
.stTabs [data-baseweb="tab-list"]{
  gap: 0.35rem !important;
  background: rgba(255,255,255,0.04) !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  padding: 0.35rem !important;
  border-radius: 999px !important;
}
.stTabs [data-baseweb="tab"]{
  background: transparent !important;
  border-radius: 999px !important;
  color: var(--muted) !important;
  border: 1px solid transparent !important;
  padding: 0.5rem 0.9rem !important;
}
.stTabs [aria-selected="true"]{
  background: rgba(255,255,255,0.08) !important;
  color: var(--text) !important;
  border: 1px solid rgba(255,255,255,0.16) !important;
}

/* Dataframe */
[data-testid="stDataFrame"]{
  border-radius: var(--r) !important;
  overflow: hidden !important;
  border: 1px solid rgba(255,255,255,0.10) !important;
  box-shadow: 0 8px 22px rgba(0,0,0,0.35);
}
[data-testid="stDataFrame"] *{
  color: rgba(255,255,255,0.88) !important;
}

/* Alerts */
.stAlert{
  border-radius: var(--r) !important;
  border: 1px solid rgba(255,255,255,0.12) !important;
  background: rgba(255,255,255,0.06) !important;
}

/* Custom cards */
.card{
  background: var(--card);
  border: 1px solid rgba(255,255,255,0.10);
  border-radius: var(--r);
  box-shadow: var(--shadow);
  padding: 1.0rem 1.05rem;
}
.card .title{
  font-weight: 700;
  color: var(--text);
  font-size: 1.05rem;
}
.card .sub{
  color: var(--muted);
  margin-top: 0.15rem;
  font-size: 0.92rem;
}

.hr{
  height: 1px;
  background: rgba(255,255,255,0.10);
  margin: 0.8rem 0 1.0rem 0;
}

/* Hide default hamburger collapse padding issues a bit */
[data-testid="collapsedControl"]{
  color: rgba(255,255,255,0.7) !important;
}
"""
st.markdown(f"<style>{THEME_CSS}</style>", unsafe_allow_html=True)


def ui_card(title: str, subtitle: str = ""):
    st.markdown(
        f"""
        <div class="card">
          <div class="title">{title}</div>
          {"<div class='sub'>"+subtitle+"</div>" if subtitle else ""}
        </div>
        """,
        unsafe_allow_html=True,
    )


def ui_hr():
    st.markdown('<div class="hr"></div>', unsafe_allow_html=True)


# ==================== RERUN ====================
def safe_rerun():
    try:
        st.rerun()
    except Exception:
        pass


# ==================== DB: Postgres (Neon) ====================
DB_URL = None
try:
    DB_URL = st.secrets.get("DB_URL", None)
except Exception:
    DB_URL = None
if not DB_URL:
    DB_URL = os.getenv("DB_URL")

if not DB_URL:
    st.error("缺少数据库连接：请在 Streamlit Secrets 或环境变量中设置 DB_URL。")
    st.stop()

engine = create_engine(DB_URL, pool_pre_ping=True, poolclass=NullPool)


# ==================== INIT DB (IDEMPOTENT) ====================
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT CHECK(role IN ('admin','user')),
        region TEXT
    )
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS quotations (
        id SERIAL PRIMARY KEY,
        序号 TEXT,
        设备材料名称 TEXT NOT NULL,
        规格或型号 TEXT,
        描述 TEXT,
        品牌 TEXT,
        单位 TEXT,
        数量确认 DOUBLE PRECISION,
        报价品牌 TEXT,
        型号 TEXT,
        设备单价 DOUBLE PRECISION,
        设备小计 DOUBLE PRECISION,
        人工包干单价 DOUBLE PRECISION,
        人工包干小计 DOUBLE PRECISION,
        综合单价汇总 DOUBLE PRECISION,
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
    )
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS misc_costs (
        id SERIAL PRIMARY KEY,
        项目名称 TEXT,
        杂费类目 TEXT,
        金额 DOUBLE PRECISION,
        币种 TEXT,
        录入人 TEXT,
        地区 TEXT,
        发生日期 TEXT
    )
    """))

    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS deleted_quotations (
        id SERIAL PRIMARY KEY,
        original_id INTEGER,
        序号 TEXT,
        设备材料名称 TEXT,
        规格或型号 TEXT,
        描述 TEXT,
        品牌 TEXT,
        单位 TEXT,
        数量确认 DOUBLE PRECISION,
        报价品牌 TEXT,
        型号 TEXT,
        设备单价 DOUBLE PRECISION,
        设备小计 DOUBLE PRECISION,
        人工包干单价 DOUBLE PRECISION,
        人工包干小计 DOUBLE PRECISION,
        综合单价汇总 DOUBLE PRECISION,
        币种 TEXT,
        原厂品牌维保期限 TEXT,
        货期 TEXT,
        备注 TEXT,
        询价人 TEXT,
        项目名称 TEXT,
        供应商名称 TEXT,
        询价日期 TEXT,
        录入人 TEXT,
        地区 TEXT,
        deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        deleted_by TEXT
    )
    """))

    conn.execute(text("""
    INSERT INTO users (username, password, role, region)
    VALUES ('admin', :pw, 'admin', 'All')
    ON CONFLICT (username) DO NOTHING
    """), {"pw": hashlib.sha256("admin123".encode()).hexdigest()})


# ==================== HELPERS ====================
HEADER_SYNONYMS = {
    "序号": "序号", "no": "序号", "index": "序号",
    "设备材料名称": "设备材料名称", "设备名称": "设备材料名称", "material": "设备材料名称", "name": "设备材料名称",
    "规格或型号": "规格或型号", "规格": "规格或型号", "model": "规格或型号", "spec": "规格或型号",
    "描述": "描述", "description": "描述",
    "品牌": "品牌", "brand": "品牌",
    "单位": "单位", "unit": "单位",
    "数量确认": "数量确认", "数量": "数量确认", "qty": "数量确认", "quantity": "数量确认",
    "报价品牌": "报价品牌", "报价": "报价品牌",
    "型号": "型号",
    "设备单价": "设备单价", "单价": "设备单价", "price": "设备单价",
    "设备小计": "设备小计", "subtotal": "设备小计",
    "币种": "币种", "currency": "币种",
    "询价人": "询价人", "项目名称": "项目名称", "供应商名称": "供应商名称",
    "询价日期": "询价日期", "录入人": "录入人", "地区": "地区"
}

DB_COLUMNS = [
    "序号", "设备材料名称", "规格或型号", "描述", "品牌", "单位", "数量确认",
    "报价品牌", "型号", "设备单价", "设备小计", "人工包干单价", "人工包干小计",
    "综合单价汇总", "币种", "原厂品牌维保期限", "货期", "备注",
    "询价人", "项目名称", "供应商名称", "询价日期", "录入人", "地区"
]


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
                best = {
                    "score": score, "header": cand, "row": start,
                    "rows_used": rows_used, "mapped": mapped, "nonempty": nonempty
                }
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
            if ser.dtype == "object":
                df_disp[col] = ser.where(ser.notna(), None).apply(lambda x: "" if x is None else str(x))
        except Exception:
            df_disp[col] = df_disp[col].where(df_disp[col].notna(), None).apply(lambda x: "" if x is None else str(x))
    return df_disp


def safe_st_dataframe(df: pd.DataFrame, height=None):
    df_disp = normalize_for_display(df)
    if height is None:
        st.dataframe(df_disp, use_container_width=True)
    else:
        st.dataframe(df_disp, height=height, use_container_width=True)


def normalize_cell(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s.lower() in ("", "nan", "none"):
        return None
    return s


# ==================== AUTH ====================
def login_form():
    ui_card("登录系统", "使用你的账号进入询价录入与查询平台")
    ui_hr()

    with st.form("login_form"):
        u = st.text_input("用户名")
        p = st.text_input("密码", type="password")
        submitted = st.form_submit_button("登录")

    if submitted:
        if not u or not p:
            st.error("请输入用户名和密码")
            return
        pw_hash = hashlib.sha256(p.encode()).hexdigest()
        with engine.begin() as conn:
            user = conn.execute(
                text("SELECT username, role, region FROM users WHERE username=:u AND password=:p"),
                {"u": u, "p": pw_hash}
            ).fetchone()
        if user:
            st.session_state["user"] = {"username": user.username, "role": user.role, "region": user.region}
            safe_rerun()
        else:
            st.error("用户名或密码错误")


def register_form():
    ui_card("注册账号", "创建一个普通用户账号；管理员账号由系统预置")
    ui_hr()

    with st.form("register_form", clear_on_submit=False):
        ru = st.text_input("新用户名", key="reg_user")
        rp = st.text_input("新密码", type="password", key="reg_pass")
        region = st.selectbox("地区", ["Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Philippines", "Others"])
        submitted = st.form_submit_button("注册")

    if submitted:
        if not ru or not rp:
            st.warning("用户名和密码不能为空")
            return
        pw_hash = hashlib.sha256(rp.encode()).hexdigest()
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO users (username,password,role,region) VALUES (:u,:p,'user',:r)"),
                    {"u": ru, "p": pw_hash, "r": region}
                )
            st.success("注册成功，请登录")
        except Exception:
            st.error("用户名已存在或数据库异常")


def logout():
    if "user" in st.session_state:
        del st.session_state["user"]
    safe_rerun()


# ==================== PAGE FLOW ====================
if "user" not in st.session_state:
    # Top hero
    left, right = st.columns([1.35, 1])
    with left:
        st.markdown("## ✨ CMI 询价录入与查询平台")
        st.caption("深色科技风格 UI（Streamlit）｜数据存储：Neon Postgres（避免 Streamlit Free 重启导致数据丢失）")
    with right:
        st.markdown(
            """
            <div class="card">
              <div class="title">使用说明</div>
              <div class="sub">
                • 先注册普通账号或使用 admin/admin123<br/>
                • 登录后可批量导入 Excel / 查询 / 下载结果<br/>
                • 管理员可删除记录、管理用户
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    ui_hr()
    tabs = st.tabs(["🔑 登录", "🧾 注册"])
    with tabs[0]:
        login_form()
    with tabs[1]:
        register_form()
    st.stop()

user = st.session_state["user"]

# ==================== TOP BAR (Merged right panel) ====================
# 左侧留空，中间导航，右侧统一信息栏（标题+当前用户）
top_l, top_m, top_blank = st.columns([1.4, 2.3, 1.1])

with top_blank:
    st.write("")  # 占位：不显示任何内容

with top_m:
    pages = ["🏠 录入页面", "📋 设备查询", "💰 杂费查询"]
    if user["role"] == "admin":
        pages.append("👑 管理员后台")
    nav_tabs = st.tabs(pages)

with top_l:
    st.markdown(
        f"""
        <div class="card">
          <div class="title">CMI 询价录入与查询平台</div>
          <div class="sub">High Tech • Effective Solution • Quick Use</div>
          <div class="hr" style="margin:0.75rem 0 0.8rem 0;"></div>
          <div class="title" style="font-size:0.98rem;">当前用户</div>
          <div class="sub">
            👤 {user["username"]}<br/>
            🏢 {user["region"]}<br/>
            🔑 {user["role"]}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button("退出登录", key="logout_btn_top"):
        logout()

ui_hr()


# Helper to map active tab -> page name
# Streamlit doesn't give "active tab index" directly; we render each tab's content in place.
# We'll simply put each page's original logic into each tab block.

# ==================== PAGE: HOME / INPUT ====================
with nav_tabs[0]:
    ui_card("录入中心", "支持 Excel 批量录入 + 手工录入（设备 / 杂费）")
    ui_hr()

    st.header("📂 Excel 批量录入")
    st.caption("系统会尝试识别上传文件的表头并给出建议映射。")

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人", "地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template.to_excel(writer, index=False)
    buf.seek(0)
    st.download_button("下载模板", buf, "quotation_template.xlsx", key="download_template")

    uploaded = st.file_uploader("上传 Excel (.xlsx)", type=["xlsx"], key="upload_excel")

    if uploaded:
        if "mapping_done" not in st.session_state:
            st.session_state["mapping_done"] = False
        if "bulk_applied" not in st.session_state:
            st.session_state["bulk_applied"] = False

        try:
            preview = pd.read_excel(uploaded, header=None, nrows=50, dtype=object)
            safe_st_dataframe(preview.head(10), height=320)
        except Exception as e:
            st.error(f"读取预览失败：{e}")
            preview = None

        if preview is not None:
            header_names, header_row_index = detect_header_from_preview(preview, max_header_rows=2, max_search_rows=8)
            raw_df_full = pd.read_excel(uploaded, header=None, dtype=object)

            if header_names is None:
                header_row_index = 0
                header_names = [str(x) if not pd.isna(x) else "" for x in raw_df_full.iloc[0].tolist()]

            data_df = raw_df_full.iloc[header_row_index + 1:].copy().reset_index(drop=True)

            if len(header_names) < data_df.shape[1]:
                header_names += [f"Unnamed_{i}" for i in range(len(header_names), data_df.shape[1])]
            elif len(header_names) > data_df.shape[1]:
                header_names = header_names[:data_df.shape[1]]

            data_df.columns = header_names

            st.markdown("**检测到的原始表头（用于映射，系统已尝试自动对应一版建议）：**")
            st.write(list(data_df.columns))

            mapping_targets = ["Ignore"] + [c for c in DB_COLUMNS if c not in ("录入人", "地区")]

            auto_defaults = {}
            for col in data_df.columns:
                auto_val = auto_map_header(col)
                auto_defaults[col] = auto_val if (auto_val and auto_val in mapping_targets) else "Ignore"

            st.markdown("系统已为每一列生成建议映射（你可以直接应用，或修改后提交）。")

            mapped_choices = {}
            with st.form("mapping_form", clear_on_submit=False):
                cols_left, cols_right = st.columns(2)
                for i, col in enumerate(data_df.columns):
                    default = auto_defaults.get(col, "Ignore")
                    container = cols_left if i % 2 == 0 else cols_right
                    sel = container.selectbox(
                        f"源列: {col}", mapping_targets,
                        index=mapping_targets.index(default) if default in mapping_targets else 0,
                        key=f"map_{i}"
                    )
                    mapped_choices[col] = sel
                submitted = st.form_submit_button("应用映射并预览")

            if submitted:
                rename_dict = {orig: mapped for orig, mapped in mapped_choices.items() if mapped != "Ignore"}
                df_mapped = data_df.rename(columns=rename_dict).copy()

                for c in DB_COLUMNS:
                    if c not in df_mapped.columns:
                        df_mapped[c] = pd.NA

                df_mapped["录入人"] = user["username"]
                df_mapped["地区"] = user["region"]
                df_for_db = df_mapped[DB_COLUMNS]

                csv_buf = io.StringIO()
                df_for_db.to_csv(csv_buf, index=False)
                st.session_state["mapping_csv"] = csv_buf.getvalue()
                st.session_state["mapping_done"] = True

                st.success("映射已保存。现在请填写全局必填信息并导入。")

    mapping_csv = st.session_state.get("mapping_csv", None)
    if mapping_csv:
        try:
            df_for_db = pd.read_csv(io.StringIO(mapping_csv), dtype=object)
            for c in DB_COLUMNS:
                if c not in df_for_db.columns:
                    df_for_db[c] = pd.NA
            df_for_db = df_for_db[DB_COLUMNS]
        except Exception as e:
            st.error(f"恢复映射数据失败：{e}")
            df_for_db = None

        st.markdown("**映射后预览（前 10 行）：**")
        if df_for_db is not None:
            safe_st_dataframe(df_for_db.head(10), height=320)
        else:
            st.info("映射数据无法预览，请重新映射。")

        if "show_global_form" not in st.session_state:
            st.session_state["show_global_form"] = False

        col_show, col_hint = st.columns([1, 6])
        if col_show.button("➡️ 填写/查看全局信息并应用导入", key="open_global_form_btn"):
            st.session_state["show_global_form"] = True
        col_hint.caption("若需要对空值统一填充（币种/项目/供应商/询价人），请展开并填写全局信息。")

        if st.session_state["show_global_form"]:
            if "bulk_values" not in st.session_state:
                st.session_state["bulk_values"] = {"project": "", "supplier": "", "enquirer": "", "date": "", "currency": ""}

            def column_has_empty_currency(df: pd.DataFrame) -> bool:
                if df is None or "币种" not in df.columns:
                    return True
                ser = df["币种"]
                return ser.map(lambda x: normalize_cell(x) is None).any()

            need_global_currency = column_has_empty_currency(df_for_db)

            st.markdown("#### 全局信息（仅填充空值）")
            with st.form("global_form_v2"):
                g1, g2, g3, g4, g5 = st.columns(5)
                g_project = g1.text_input("项目名称", value=st.session_state["bulk_values"].get("project", ""))
                g_supplier = g2.text_input("供应商名称", value=st.session_state["bulk_values"].get("supplier", ""))
                g_enquirer = g3.text_input("询价人", value=st.session_state["bulk_values"].get("enquirer", ""))

                default_date = st.session_state["bulk_values"].get("date", "")
                try:
                    g_date = g4.date_input("询价日期", value=pd.to_datetime(default_date).date() if default_date else date.today())
                except Exception:
                    g_date = g4.date_input("询价日期", value=date.today())

                g_currency = None
                if need_global_currency:
                    currency_options = ["", "IDR", "USD", "RMB", "SGD", "MYR", "THB"]
                    curr_default = st.session_state["bulk_values"].get("currency", "")
                    default_idx = currency_options.index(curr_default) if curr_default in currency_options else 0
                    g_currency = g5.selectbox("币种（用于填充空值）", currency_options, index=default_idx)
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
                    df_for_db2 = pd.read_csv(io.StringIO(st.session_state["mapping_csv"]), dtype=object)
                    for c in DB_COLUMNS:
                        if c not in df_for_db2.columns:
                            df_for_db2[c] = pd.NA
                    df_for_db2 = df_for_db2[DB_COLUMNS]
                except Exception as e:
                    st.error(f"恢复映射数据失败：{e}")
                    df_for_db2 = None

                if df_for_db2 is None:
                    st.error("映射数据丢失，无法继续导入。")
                else:
                    df_final = df_for_db2.copy()
                    g = st.session_state["bulk_values"]

                    def fill_empty(col_name, value):
                        if col_name not in df_final.columns:
                            df_final[col_name] = pd.NA
                        mask = df_final[col_name].map(lambda x: normalize_cell(x) is None)
                        if mask.any():
                            df_final.loc[mask, col_name] = value

                    fill_empty("项目名称", str(g["project"]))
                    fill_empty("供应商名称", str(g["supplier"]))
                    fill_empty("询价人", str(g["enquirer"]))
                    fill_empty("询价日期", str(g["date"]))
                    if need_global_currency and g.get("currency"):
                        fill_empty("币种", str(g["currency"]))

                    required_nonprice = ["项目名称", "供应商名称", "询价人", "设备材料名称", "币种", "询价日期"]
                    check_nonprice = df_final[required_nonprice].applymap(normalize_cell)
                    missing_nonprice = check_nonprice.isna().any(axis=1)

                    def price_has_value(row) -> bool:
                        v1 = normalize_cell(row.get("设备单价", None))
                        v2 = normalize_cell(row.get("人工包干单价", None))
                        return (v1 is not None) or (v2 is not None)

                    price_mask = df_final.apply(price_has_value, axis=1)
                    rows_invalid_mask = missing_nonprice | (~price_mask)

                    df_valid = df_final[~rows_invalid_mask].copy()
                    df_invalid = df_final[rows_invalid_mask].copy()

                    imported_count = 0
                    if not df_valid.empty:
                        try:
                            df_to_store = df_valid.dropna(how="all").drop_duplicates().reset_index(drop=True)
                            with engine.begin() as conn:
                                df_to_store.to_sql("quotations", conn, if_exists="append", index=False, method="multi")
                            imported_count = len(df_to_store)
                            st.success(f"✅ 已导入 {imported_count} 条有效记录。")
                        except Exception as e:
                            st.error(f"导入有效记录时发生错误：{e}")
                    else:
                        st.info("没有找到满足总体必填条件的记录可导入。")

                    if not df_invalid.empty:
                        st.warning(f"以下 {len(df_invalid)} 条记录缺少总体必填字段，未被导入，请修正后重新导入：")
                        safe_st_dataframe(df_invalid.head(50), height=360)
                        buf_bad = io.BytesIO()
                        with pd.ExcelWriter(buf_bad, engine="openpyxl") as w:
                            df_invalid.to_excel(w, index=False)
                        buf_bad.seek(0)
                        st.download_button("📥 下载未通过记录（用于修正）", buf_bad, "invalid_rows.xlsx")

                    st.session_state["bulk_applied"] = False

    ui_hr()
    st.header("✏️ 手工录入设备")
    with st.form("manual_add_form_original", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input("项目名称")
        sup = col2.text_input("供应商名称")
        inq = col3.text_input("询价人")
        name = st.text_input("设备材料名称")
        brand = st.text_input("品牌（可选）")
        qty = st.number_input("数量确认", min_value=0.0)
        price = st.number_input("设备单价", min_value=0.0)
        labor_price = st.number_input("人工包干单价", min_value=0.0)
        cur = st.selectbox("币种", ["IDR", "USD", "RMB", "SGD", "MYR", "THB"])
        desc = st.text_area("描述")
        date_inq = st.date_input("询价日期", value=date.today())
        submit_manual = st.form_submit_button("添加记录（手动）")

    if submit_manual:
        if not (pj and sup and inq and name):
            st.error("必填项不能为空：项目名称、供应商名称、询价人、设备材料名称")
        else:
            if not (price > 0 or labor_price > 0):
                st.error("请至少填写 设备单价 或 人工包干单价（两者至少填一项，且大于0）。")
            else:
                try:
                    with engine.begin() as conn:
                        conn.execute(text("""
                            INSERT INTO quotations
                            (项目名称,供应商名称,询价人,设备材料名称,品牌,数量确认,设备单价,人工包干单价,币种,描述,录入人,地区,询价日期)
                            VALUES (:p,:s,:i,:n,:b,:q,:pr,:lp,:c,:d,:u,:reg,:dt)
                        """), {
                            "p": pj, "s": sup, "i": inq, "n": name,
                            "b": brand if brand is not None else "",
                            "q": float(qty),
                            "pr": float(price) if price > 0 else None,
                            "lp": float(labor_price) if labor_price > 0 else None,
                            "c": cur, "d": desc,
                            "u": user["username"], "reg": user["region"], "dt": str(date_inq)
                        })
                    st.success("✅ 手工记录已添加")
                except Exception as e:
                    st.error(f"添加记录失败：{e}")

    ui_hr()
    st.header("💰 手工录入杂费")
    with st.form("manual_misc_form", clear_on_submit=True):
        mcol1, mcol2, mcol3 = st.columns(3)
        misc_project = mcol1.text_input("项目名称")
        misc_category = mcol2.text_input("杂费类目（例如运输/安装/税费）")
        misc_amount = mcol3.number_input("金额", min_value=0.0, format="%f")
        mc1, mc2 = st.columns(2)
        misc_currency = mc1.selectbox("币种", ["IDR", "USD", "RMB", "SGD", "MYR", "THB"])
        misc_note = mc2.text_input("备注（可选）")
        misc_date = st.date_input("发生日期", value=date.today())
        submit_misc = st.form_submit_button("添加杂费记录")

    if submit_misc:
        if not (misc_project and misc_category) or misc_amount is None:
            st.error("请填写项目名称、杂费类目和金额")
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO misc_costs
                        (项目名称, 杂费类目, 金额, 币种, 录入人, 地区, 发生日期)
                        VALUES (:pj, :cat, :amt, :cur, :user, :region, :occ_date)
                    """), {
                        "pj": misc_project,
                        "cat": misc_category + (f" | {misc_note}" if misc_note else ""),
                        "amt": float(misc_amount),
                        "cur": misc_currency,
                        "user": user["username"],
                        "region": user["region"],
                        "occ_date": str(misc_date)
                    })
                st.success("✅ 杂费记录已添加")
            except Exception as e:
                st.error(f"添加杂费记录失败：{e}")


# ==================== PAGE: SEARCH QUOTATIONS ====================
with nav_tabs[1]:
    ui_card("设备查询", "按关键词 / 项目 / 供应商 / 品牌 / 地区筛选，并支持导出 Excel")
    ui_hr()

    st.header("📋 设备查询")

    kw = st.text_input("关键词（多个空格分词）", key="search_kw")
    search_fields = st.multiselect(
        "搜索字段（留空为默认）",
        ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称", "地区"],
        key="search_fields"
    )
    pj_filter = st.text_input("按项目名称过滤", key="search_pj")
    sup_filter = st.text_input("按供应商名称过滤", key="search_sup")
    brand_filter = st.text_input("按品牌过滤", key="search_brand")
    cur_filter = st.selectbox("币种", ["全部", "IDR", "USD", "RMB", "SGD", "MYR", "THB"], index=0, key="search_cur")

    regions_options = ["全部", "Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Philippines", "Others", "All"]
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
            fields = search_fields if search_fields else ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称"]
            for i, t in enumerate(tokens):
                ors = []
                for j, f in enumerate(fields):
                    pname = f"kw_{i}_{j}"
                    ors.append(f"LOWER({f}) LIKE :{pname}")
                    params[pname] = f"%{t.lower()}%"
                conds.append("(" + " OR ".join(ors) + ")")

        sql = "SELECT * FROM quotations"
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY id DESC"

        try:
            df = pd.read_sql(text(sql), engine, params=params)
        except Exception as e:
            st.error(f"查询失败：{e}")
            df = pd.DataFrame()

        if df.empty:
            st.info("未找到符合条件的记录。")
        else:
            safe_st_dataframe(df, height=520)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            buf.seek(0)
            st.download_button("下载结果", buf, "设备查询结果.xlsx", key="download_search")

            # Price stats (unchanged)
            try:
                df_prices = df.copy()
                device_price_col = "设备单价"
                labor_price_col = "人工包干单价"
                name_col = "设备材料名称"

                df_prices[device_price_col] = pd.to_numeric(df_prices.get(device_price_col), errors="coerce")
                df_prices[labor_price_col] = pd.to_numeric(df_prices.get(labor_price_col), errors="coerce")

                overall = {
                    "dev_mean": df_prices[device_price_col].mean(skipna=True),
                    "dev_min": df_prices[device_price_col].min(skipna=True),
                    "lab_mean": df_prices[labor_price_col].mean(skipna=True),
                    "lab_min": df_prices[labor_price_col].min(skipna=True),
                }

                def fmt(v):
                    return "-" if (v is None or (isinstance(v, float) and pd.isna(v))) else f"{v:,.2f}"

                ui_hr()
                st.markdown("### 当前查询 — 价格统计概览（基于返回记录）")
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("设备单价 — 均价", fmt(overall["dev_mean"]))
                c2.metric("设备单价 — 最低价", fmt(overall["dev_min"]))
                c3.metric("人工包干单价 — 均价", fmt(overall["lab_mean"]))
                c4.metric("人工包干单价 — 最低价", fmt(overall["lab_min"]))

                if not pd.isna(overall["dev_min"]):
                    dev_min_rows = df_prices[df_prices[device_price_col] == overall["dev_min"]].copy()
                    st.markdown("#### 设备单价 — 历史最低价对应记录（可能多条并列）")
                    safe_st_dataframe(dev_min_rows.reset_index(drop=True), height=260)

                if not pd.isna(overall["lab_min"]):
                    lab_min_rows = df_prices[df_prices[labor_price_col] == overall["lab_min"]].copy()
                    st.markdown("#### 人工包干单价 — 历史最低价对应记录（可能多条并列）")
                    safe_st_dataframe(lab_min_rows.reset_index(drop=True), height=260)

                if name_col in df_prices.columns:
                    agg = df_prices.groupby(name_col).agg(
                        设备单价_均价=(device_price_col, lambda s: s.mean(skipna=True)),
                        设备单价_最低=(device_price_col, lambda s: s.min(skipna=True)),
                        人工包干单价_均价=(labor_price_col, lambda s: s.mean(skipna=True)),
                        人工包干单价_最低=(labor_price_col, lambda s: s.min(skipna=True)),
                        样本数=(device_price_col, "count")
                    ).reset_index()
                    st.markdown("#### 按设备名称分组 — 均价 / 最低价")
                    safe_st_dataframe(agg.sort_values(by="设备单价_均价", ascending=True).head(200), height=360)
            except Exception as e:
                st.warning(f"计算价格统计时发生异常：{e}")

            # Admin delete by id (unchanged)
            if user["role"] == "admin":
                ui_hr()
                st.markdown("### ⚠️ 管理员删除（按 id 删除）")
                choices = []
                for _, row in df.iterrows():
                    rid = int(row["id"])
                    proj = str(row.get("项目名称", ""))[:40]
                    name = str(row.get("设备材料名称", ""))[:60]
                    brand = str(row.get("品牌", ""))[:30]
                    choices.append(f"{rid} | {proj} | {name} | {brand}")

                with st.form("admin_delete_form_pg", clear_on_submit=False):
                    selected = st.multiselect("选中要删除的记录", choices, key="admin_delete_selected_pg")
                    confirm = st.checkbox("我确认删除所选记录（不可恢复）", key="admin_delete_confirm_pg")
                    submit_del = st.form_submit_button("删除所选记录（管理员）", key="admin_delete_submit_pg")

                if submit_del:
                    if not selected:
                        st.warning("请先选择要删除的记录。")
                    elif not confirm:
                        st.warning("请勾选确认框以执行删除。")
                    else:
                        try:
                            selected_ids = [int(s.split("|", 1)[0].strip()) for s in selected]
                        except Exception as e:
                            st.error(f"解析所选 id 失败：{e}")
                            selected_ids = []

                        if not selected_ids:
                            st.warning("无有效 id，取消删除。")
                        else:
                            placeholders = ",".join(str(int(i)) for i in selected_ids)
                            try:
                                with engine.begin() as conn:
                                    conn.execute(text(f"""
                                        INSERT INTO deleted_quotations (
                                            original_id, 序号, 设备材料名称, 规格或型号, 描述, 品牌, 单位, 数量确认,
                                            报价品牌, 型号, 设备单价, 设备小计, 人工包干单价, 人工包干小计, 综合单价汇总,
                                            币种, 原厂品牌维保期限, 货期, 备注, 询价人, 项目名称, 供应商名称, 询价日期, 录入人, 地区,
                                            deleted_by
                                        )
                                        SELECT
                                            id, 序号, 设备材料名称, 规格或型号, 描述, 品牌, 单位, 数量确认,
                                            报价品牌, 型号, 设备单价, 设备小计, 人工包干单价, 人工包干小计, 综合单价汇总,
                                            币种, 原厂品牌维保期限, 货期, 备注, 询价人, 项目名称, 供应商名称, 询价日期, 录入人, 地区,
                                            :user
                                        FROM quotations WHERE id IN ({placeholders})
                                    """), {"user": user["username"]})
                                    conn.execute(text(f"DELETE FROM quotations WHERE id IN ({placeholders})"))
                                st.success("✅ 已删除并归档所选记录。")
                                safe_rerun()
                            except Exception as e:
                                st.error(f"删除/归档失败：{e}")
            else:
                st.info("仅管理员可删除记录。")


# ==================== PAGE: SEARCH MISC ====================
with nav_tabs[2]:
    ui_card("杂费查询", "按项目名称检索杂费记录，支持导出 Excel")
    ui_hr()

    st.header("💰 杂费查询")
    pj2 = st.text_input("按项目名称过滤", key="misc_pj")

    if st.button("🔍 搜索杂费", key="misc_search"):
        params = {"pj": f"%{pj2.lower()}%"}
        sql = """
            SELECT id, 项目名称, 杂费类目, 金额, 币种, 录入人, 地区, 发生日期
            FROM misc_costs
            WHERE LOWER(项目名称) LIKE :pj
            ORDER BY id DESC
        """
        if user["role"] != "admin":
            sql = sql.replace("ORDER BY id DESC", "AND 地区 = :r ORDER BY id DESC")
            params["r"] = user["region"]

        try:
            df2 = pd.read_sql(text(sql), engine, params=params)
        except Exception as e:
            st.error(f"查询失败：{e}")
            df2 = pd.DataFrame()

        safe_st_dataframe(df2, height=520)
        if not df2.empty:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as writer:
                df2.to_excel(writer, index=False)
            buf2.seek(0)
            st.download_button("下载杂费结果", buf2, "misc_costs.xlsx", key="download_misc")


# ==================== PAGE: ADMIN ====================
if user["role"] == "admin":
    with nav_tabs[3]:
        ui_card("管理员后台", "用户管理：查看 / 修改地区 / 删除账号（保护当前用户与默认 admin）")
        ui_hr()

        st.header("👑 管理员后台 — 用户管理")
        users_df = pd.read_sql(text("SELECT id, username, role, region FROM users ORDER BY id"), engine)
        safe_st_dataframe(users_df, height=420)

        ui_hr()
        st.subheader("🛠️ 修改用户地区（Region）")

        region_options = ["Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Philippines", "Others", "All"]
        user_choices = [f"{row['id']} | {row['username']} | {row['role']} | {row['region']}" for _, row in users_df.iterrows()]

        with st.form("admin_update_user_region_form"):
            target = st.selectbox("选择要修改的用户", user_choices, key="admin_update_user_select")
            new_region = st.selectbox("新地区", region_options, key="admin_update_user_region")
            confirm_update = st.checkbox("我确认要修改该用户地区", key="admin_update_user_confirm")
            submit_update = st.form_submit_button("更新地区")

        if submit_update:
            try:
                target_id = int(target.split("|", 1)[0].strip())
                target_row = users_df[users_df["id"] == target_id]
                if target_row.empty:
                    st.error("未找到该用户，请刷新页面。")
                else:
                    target_username = str(target_row.iloc[0]["username"])
                    target_role = str(target_row.iloc[0]["role"])

                    if target_role == "admin" and target_username == "admin":
                        st.warning("系统默认 admin 不建议修改地区。")
                    elif not confirm_update:
                        st.warning("请勾选确认框后再更新。")
                    else:
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE users SET region=:r WHERE id=:id"), {"r": new_region, "id": target_id})
                        st.success(f"✅ 已更新用户 {target_username} 的地区为：{new_region}")
                        safe_rerun()
            except Exception as e:
                st.error(f"更新失败：{e}")

        ui_hr()
        st.subheader("🗑️ 删除用户账号")
        st.caption("说明：删除账号不会自动删除该用户已录入的报价/杂费数据（数据仍保留在 quotations / misc_costs 表中）。")

        protected_usernames = {user["username"], "admin"}
        deletable_rows = users_df[~users_df["username"].isin(protected_usernames)].copy()

        if deletable_rows.empty:
            st.info("当前没有可删除的用户（已保护当前登录用户与默认 admin）。")
        else:
            del_choices = [f"{row['id']} | {row['username']} | {row['role']} | {row['region']}" for _, row in deletable_rows.iterrows()]

            with st.form("admin_delete_users_form"):
                selected = st.multiselect("选择要删除的用户（可多选）", del_choices, key="admin_delete_users_select")
                confirm_del = st.checkbox("我确认删除所选用户（不可恢复）", key="admin_delete_users_confirm")
                submit_del = st.form_submit_button("删除用户")

            if submit_del:
                if not selected:
                    st.warning("请先选择要删除的用户。")
                elif not confirm_del:
                    st.warning("请勾选确认框后再删除。")
                else:
                    try:
                        del_ids = [int(s.split("|", 1)[0].strip()) for s in selected]
                        check_df = users_df[users_df["id"].isin(del_ids)]
                        bad = check_df[check_df["username"].isin(protected_usernames)]
                        if not bad.empty:
                            st.error("所选用户包含受保护账号（当前登录用户或默认 admin），已拒绝删除。")
                        else:
                            placeholders = ",".join(str(i) for i in del_ids)
                            with engine.begin() as conn:
                                conn.execute(text(f"DELETE FROM users WHERE id IN ({placeholders})"))
                            st.success(f"✅ 已删除 {len(del_ids)} 个用户账号")
                            safe_rerun()
                    except Exception as e:
                        st.error(f"删除失败：{e}")
