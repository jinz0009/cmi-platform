# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib, io, re, math

# ============ 基础配置 ============
st.set_page_config(page_title="CMI 询价录入与查询平台", layout="wide")

# Streamlit Cloud 每次启动会重置内存，因此使用本地 SQLite（临时存储）
engine = create_engine("sqlite:///quotation.db")

# ============ 初始化数据库 ============
with engine.begin() as conn:
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT CHECK(role IN ('admin','user')),
        region TEXT
    )
    """))
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
    )
    """))
    conn.execute(text("""
    CREATE TABLE IF NOT EXISTS misc_costs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        项目名称 TEXT,
        杂费类目 TEXT,
        金额 REAL,
        币种 TEXT,
        录入人 TEXT,
        地区 TEXT
    )
    """))
    conn.execute(text("""
    INSERT OR IGNORE INTO users (username, password, role, region)
    VALUES ('admin', :pw, 'admin', 'All')
    """), {"pw": hashlib.sha256("admin123".encode()).hexdigest()})

# ============ 帮助：表头映射字典（可扩展） ============
HEADER_SYNONYMS = {
    "序号": "序号", "no": "序号", "index": "序号",
    "设备材料名称": "设备材料名称", "设备名称": "设备材料名称",
    "material": "设备材料名称", "material name": "设备材料名称",
    "item": "设备材料名称", "name": "设备材料名称",
    "规格或型号": "规格或型号", "规格": "规格或型号", "model": "规格或型号", "spec": "规格或型号",
    "描述": "描述", "description": "描述",
    "品牌": "品牌", "brand": "品牌",
    "单位": "单位", "unit": "单位",
    "数量确认": "数量确认", "数量": "数量确认", "qty": "数量确认", "quantity": "数量确认",
    "报价品牌": "报价品牌", "报价": "报价品牌",
    "型号": "型号",
    "设备单价": "设备单价", "单价": "设备单价", "price": "设备单价", "unit price": "设备单价",
    "设备小计": "设备小计", "subtotal": "设备小计",
    "人工包干单价": "人工包干单价", "人工包干小计": "人工包干小计", "综合单价汇总": "综合单价汇总",
    "币种": "币种", "currency": "币种",
    "原厂品牌维保期限": "原厂品牌维保期限", "货期": "货期",
    "备注": "备注", "remark": "备注", "notes": "备注",
    "询价人": "询价人", "enquirer": "询价人", "inquirer": "询价人",
    "项目名称": "项目名称", "project": "项目名称", "project name": "项目名称",
    "供应商名称": "供应商名称", "supplier": "供应商名称", "vendor": "供应商名称",
    "询价日期": "询价日期", "date": "询价日期",
    "录入人": "录入人", "地区": "地区",
    # 带币种/括号的常见写法（示例）
    "设备单价（idr）": "设备单价", "设备单价(idr)": "设备单价", "设备单价（rmb）": "设备单价",
    "设备单价(rmb)": "设备单价", "设备小计（idr）": "设备小计", "设备小计(idr)": "设备小计",
    "设备小计（rmb）": "设备小计", "设备小计(rmb)": "设备小计",
    "人工包干单价（idr）": "人工包干单价", "人工包干单价(idr)": "人工包干单价",
    "人工包干小计（idr）": "人工包干小计", "人工包干小计(idr)": "人工包干小计",
    "综合单价汇总（idr）": "综合单价汇总", "综合单价汇总(idr)": "综合单价汇总",
    "price (idr)": "设备单价", "subtotal (idr)": "设备小计",
}

DB_COLUMNS = [
    "序号","设备材料名称","规格或型号","描述","品牌","单位","数量确认",
    "报价品牌","型号","设备单价","设备小计","人工包干单价","人工包干小计",
    "综合单价汇总","币种","原厂品牌维保期限","货期","备注",
    "询价人","项目名称","供应商名称","询价日期","录入人","地区"
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
        k_norm = re.sub(r"[\s\-\_：:（）()]+", " ", k.lower()).strip()
        if h_norm == k_norm:
            return v
    for k, v in HEADER_SYNONYMS.items():
        if k.lower() in h or h in k.lower():
            return v
    words = re.findall(r"[a-zA-Z\u4e00-\u9fff]+", h)
    for w in words:
        for k, v in HEADER_SYNONYMS.items():
            if w == k.lower():
                return v
    return None

def detect_header_from_preview(df_preview: pd.DataFrame, max_header_rows=2, max_search_rows=8):
    nrows = df_preview.shape[0]
    ncols = df_preview.shape[1]
    search_rows = min(max_search_rows, nrows)
    best = {"score": -1, "header": None, "row": None, "rows_used": 1}
    for start in range(search_rows):
        for rows_used in range(1, max_header_rows + 1):
            if start + rows_used > nrows:
                continue
            cand = []
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
                cand.append(header_text)
            mapped_count = 0
            nonempty_count = sum(1 for x in cand if x)
            for h in cand:
                if h and auto_map_header(h):
                    mapped_count += 1
            score = mapped_count + 0.5 * nonempty_count
            if score > best["score"]:
                best = {"score": score, "header": cand, "row": start, "rows_used": rows_used, "mapped": mapped_count, "nonempty": nonempty_count}
    if best["header"] is not None:
        if best["mapped"] >= 2 or (best["nonempty"] > 0 and best["mapped"] / best["nonempty"] >= 0.3):
            return best["header"], best["row"] + best["rows_used"] - 1
    return None, None

# 规范化用于显示的 DataFrame（避免 ArrowTypeError）
def normalize_for_display(df: pd.DataFrame) -> pd.DataFrame:
    df_disp = df.copy()
    for col in df_disp.columns:
        try:
            ser = df_disp[col]
            if ser.dtype != "object":
                # keep numeric/datetime as-is (usually safe)
                continue
            non_null = ser.dropna()
            if non_null.empty:
                df_disp[col] = ser.where(ser.notna(), "").astype(str)
                continue
            has_bytes = any(isinstance(x, (bytes, bytearray, memoryview)) for x in non_null)
            types_seen = {type(x) for x in non_null}
            multiple_types = len(types_seen) > 1
            if has_bytes or multiple_types:
                df_disp[col] = ser.where(ser.notna(), None).apply(lambda x: "" if x is None else str(x))
            else:
                df_disp[col] = ser.where(ser.notna(), None).apply(lambda x: "" if x is None else x)
        except Exception:
            df_disp[col] = df_disp[col].where(df_disp[col].notna(), None).apply(lambda x: "" if x is None else str(x))
    return df_disp

# 安全显示 DataFrame（修复 height=None 问题并做降级）
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
            try:
                df2[col] = df2[col].where(df2[col].notna(), None).apply(lambda x: "" if x is None else str(x))
            except Exception:
                df2[col] = df2[col].astype(str).fillna("")
        if height is None:
            st.dataframe(df2)
        else:
            st.dataframe(df2, height=height)

# ============ 登录注册逻辑 ============
def login():
    st.subheader("🔐 用户登录")
    username = st.text_input("用户名")
    password = st.text_input("密码", type="password")
    if st.button("登录"):
        pw_hash = hashlib.sha256(password.encode()).hexdigest()
        with engine.begin() as conn:
            user = conn.execute(
                text("SELECT * FROM users WHERE username=:u AND password=:p"),
                {"u": username, "p": pw_hash}
            ).fetchone()
        if user:
            st.session_state["user"] = {"username": username, "role": user.role, "region": user.region}
            st.success(f"✅ 登录成功！欢迎 {username}（{user.region}）")
            st.rerun()
        else:
            st.error("❌ 用户名或密码错误")

def register():
    st.subheader("🧾 注册新用户")
    username = st.text_input("新用户名")
    password = st.text_input("新密码", type="password")
    region = st.selectbox("所属地区", ["Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others"])
    if st.button("注册"):
        if not username or not password:
            st.warning("请输入用户名和密码")
        else:
            pw_hash = hashlib.sha256(password.encode()).hexdigest()
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text("INSERT INTO users (username, password, role, region) VALUES (:u, :p, 'user', :r)"),
                        {"u": username, "p": pw_hash, "r": region}
                    )
                st.success("✅ 注册成功，请返回登录页。")
            except Exception:
                st.error("❌ 用户名已存在")

def logout():
    st.session_state.clear()
    st.rerun()

# ============ 页面切换 ============
if "user" not in st.session_state:
    tab = st.tabs(["🔑 登录", "🧾 注册"])
    with tab[0]:
        login()
    with tab[1]:
        register()
    st.stop()

user = st.session_state["user"]
st.sidebar.markdown(f"👤 **{user['username']}**  \n🏢 地区：{user['region']}  \n🔑 角色：{user['role']}")
if st.sidebar.button("🚪 退出登录"):
    logout()

page = st.sidebar.radio("导航", ["🏠 主页面", "📋 设备查询", "💰 杂费查询", "👑 管理员后台"] if user["role"]=="admin" else ["🏠 主页面", "📋 设备查询", "💰 杂费查询"])

# ============ 主页面：录入 ============
if page == "🏠 主页面":
    st.title("📊 询价录入与查询平台")

    st.header("📂 Excel 批量录入（智能表头映射）")
    st.caption("系统会尝试识别上传文件的表头（支持前几行为合并单元格或标题），并给出建议映射。系统会先自动对应一版建议，你可以按列修改并看到哪些目标列未被提供或无值。")

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人","地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        template.to_excel(w, index=False)
    buf.seek(0)
    st.download_button("📥 下载模板", buf, "quotation_template.xlsx")

    uploaded = st.file_uploader("上传 Excel 文件（.xlsx）", type=["xlsx"])
    if uploaded:
        try:
            preview = pd.read_excel(uploaded, header=None, nrows=50, dtype=object)
        except Exception as e:
            st.error(f"读取 Excel 预览失败：{e}")
            preview = None

        if preview is not None:
            st.markdown("**用于识别表头的前几行预览（仅展示）：**")
            safe_st_dataframe(preview.head(10))

            header_names, header_row_index = detect_header_from_preview(preview, max_header_rows=2, max_search_rows=8)
            raw_df_full = pd.read_excel(uploaded, header=None, dtype=object)

            if header_names is None:
                st.info("未能自动识别表头，请手动映射（系统已把第一行作为候选）。")
                header_row_index = 0
                header_names = [str(x) if not pd.isna(x) else "" for x in raw_df_full.iloc[0].tolist()]
            else:
                st.success(f"已检测到表头（结束于第 {header_row_index+1} 行），系统已生成建议映射：")
                st.write(header_names)

            header_names = [str(x).strip() if (x is not None and not pd.isna(x)) else "" for x in header_names]

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
            with st.form("mapping_form"):
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

                unmapped_targets = [t for t in DB_COLUMNS if t not in ("录入人","地区") and t not in target_sources.keys()]

                dup_targets = {t: s for t, s in target_sources.items() if len(s) > 1}
                if dup_targets:
                    dup_messages = []
                    for t, srcs in dup_targets.items():
                        dup_messages.append(f"目标列 '{t}' 被多个源列映射: {', '.join(srcs)}")
                    st.error("检测到多个源列映射同一目标列（这是不允许的）。请在映射中只为每个目标选择一个源列。\n\n" + "\n".join(dup_messages))
                    st.stop()

                mapped_but_empty = []
                for tgt, srcs in target_sources.items():
                    has_value = False
                    for s in srcs:
                        if s in data_df.columns:
                            col_series = data_df[s].astype(object).where(~data_df[s].astype(str).str.strip().isin(["", "nan", "none"]), pd.NA)
                            if col_series.dropna().size > 0:
                                has_value = True
                                break
                    if not has_value:
                        mapped_but_empty.append(tgt)

                rename_dict = {orig: mapped for orig, mapped in mapped_choices.items() if mapped != "Ignore"}
                df_mapped = data_df.rename(columns=rename_dict).copy()
                for c in DB_COLUMNS:
                    if c not in df_mapped.columns:
                        df_mapped[c] = pd.NA
                df_mapped["录入人"] = user["username"]
                df_mapped["地区"] = user["region"]
                df_for_db = df_mapped[DB_COLUMNS]

                st.markdown("**映射后预览（前 10 行）：**")
                safe_st_dataframe(df_for_db.head(10))

                # 全局信息：在单独表单内提交，避免只输入一项就继续
                st.markdown("请先填写以下全局必填信息（会应用到所有导入记录），填写完后点击“应用全局并继续校验”：")
                with st.form("global_form"):
                    col_a, col_b, col_c, col_d = st.columns(4)
                    global_project = col_a.text_input("项目名称", key="bulk_project")
                    global_supplier = col_b.text_input("供应商名称", key="bulk_supplier")
                    global_enquirer = col_c.text_input("询价人", key="bulk_enquirer")
                    global_date = col_d.date_input("询价日期", key="bulk_date")
                    apply_global = st.form_submit_button("应用全局并继续校验")

                if not apply_global:
                    st.info("请填写全局必填信息并点击“应用全局并继续校验”以继续。")
                    st.stop()

                if not (global_project and global_supplier and global_enquirer and global_date):
                    st.error("必须填写：项目名称、供应商名称、询价人和询价日期，才能继续导入。")
                    st.stop()

                # 将四个全局字段写入所有行（覆盖或改为 fillna 行为可按需切换）
                df_final = df_for_db.copy()
                df_final["项目名称"] = str(global_project)
                df_final["供应商名称"] = str(global_supplier)
                df_final["询价人"] = str(global_enquirer)
                df_final["询价日期"] = str(global_date)

                overall_required = ["项目名称","供应商名称","询价人","设备材料名称","品牌","设备单价","币种","询价日期"]

                def normalize_cell(x):
                    if pd.isna(x):
                        return None
                    s = str(x).strip()
                    if s.lower() in ("", "nan", "none"):
                        return None
                    return s

                # 使用规范化显示副本进行展示和问题行高亮，但业务校验基于 normalize_cell 判空
                df_final_disp = normalize_for_display(df_final)
                check_df = df_final.applymap(normalize_cell)[overall_required]
                rows_missing_mask = check_df.isna().any(axis=1)
                if rows_missing_mask.any():
                    bad = df_final_disp[rows_missing_mask]
                    st.error(f"检测到部分记录缺少总体必填字段（{', '.join(overall_required)} 中至少一项）：共 {len(bad)} 条记录有缺项，已中止导入。请检查源数据或补全后再导入。")
                    safe_st_dataframe(bad.head(20))
                    st.stop()

                st.markdown("**预备导入的最终预览（前 10 行）：**")
                safe_st_dataframe(df_final_disp.head(10))

                if st.button("✅ 确认并导入这些记录"):
                    try:
                        df_to_store = df_final.dropna(how="all").drop_duplicates().reset_index(drop=True)
                        final_check = df_to_store[["设备材料名称","品牌","设备单价","币种"]].applymap(normalize_cell)
                        empty_rows_mask = final_check.isna().any(axis=1)
                        if empty_rows_mask.any():
                            bad2 = normalize_for_display(df_to_store[empty_rows_mask])
                            st.error("检测到部分记录在业务必填字段（设备材料名称、品牌、设备单价、币种）仍为空，已中止导入。请检查源文件或手工补全后再导入。")
                            safe_st_dataframe(bad2.head(20))
                        else:
                            with engine.begin() as conn:
                                df_to_store.to_sql("quotations", conn, if_exists="append", index=False)
                            st.success(f"✅ 导入成功，共 {len(df_to_store)} 条记录。")
                    except Exception as e:
                        st.error(f"导入失败：{e}")

    # 2️⃣ 手工录入
    st.header("✏️ 设备手工录入")
    with st.form("device_form"):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input("项目名称")
        sup = col2.text_input("供应商名称")
        inq = col3.text_input("询价人")
        name = st.text_input("设备材料名称")
        brand = st.text_input("品牌")
        qty = st.number_input("数量确认", min_value=0.0)
        price = st.number_input("设备单价", min_value=0.0)
        cur = st.selectbox("币种", ["IDR","USD","RMB","SGD","MYR","THB"])
        desc = st.text_area("描述（可选）")
        remark = st.text_area("备注（可选）")
        date = st.date_input("询价日期")
        ok = st.form_submit_button("➕ 添加记录")
    if ok:
        if not (pj and sup and inq and name and brand):
            st.error("❌ 必填项不能为空")
        else:
            with engine.begin() as conn:
                conn.execute(text("""
                INSERT INTO quotations (项目名称,供应商名称,询价人,设备材料名称,品牌,数量确认,
                设备单价,币种,描述,备注,询价日期,录入人,地区)
                VALUES (:p,:s,:i,:n,:b,:q,:pr,:c,:d,:r,:dt,:u,:reg)
                """), {"p": pj,"s": sup,"i": inq,"n": name,"b": brand,"q": qty,"pr": price,"c": cur,
                        "d": desc,"r": remark,"dt": str(date),"u": user["username"],"reg": user["region"]})
            st.success("✅ 已添加记录。")

    # 3️⃣ 杂费录入
    st.header("💰 杂费录入")
    with st.form("misc_form"):
        pj = st.text_input("项目名称")
        cat = st.text_input("杂费类目")
        amt = st.number_input("金额", min_value=0.0)
        cur = st.selectbox("币种", ["IDR","USD","RMB","SGD","MYR","THB"])
        ok = st.form_submit_button("➕ 添加杂费")
    if ok:
        if not pj or not cat:
            st.error("❌ 必填项不能为空")
        else:
            with engine.begin() as conn:
                conn.execute(text("""
                INSERT INTO misc_costs (项目名称,杂费类目,金额,币种,录入人,地区)
                VALUES (:p,:c,:a,:cur,:u,:r)
                """), {"p": pj,"c": cat,"a": amt,"cur": cur,"u": user["username"],"r": user["region"]})
            st.success("✅ 杂费已添加。")

# ============ 查询模块 ============
elif page == "📋 设备查询":
    st.header("📋 设备查询")
    kw = st.text_input("关键词（按选定字段搜索）")
    search_fields = st.multiselect("选择要在其内搜索关键词（不选则在默认字段搜索）",
                                   ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称", "地区"])
    pj = st.text_input("按项目名称过滤（可选）")
    sup = st.text_input("按供应商名称过滤（可选）")
    brand_f = st.text_input("按品牌过滤（可选）")
    cur = st.selectbox("币种", ["全部","IDR","USD","RMB","SGD","MYR","THB"], index=0)

    regions_options = ["全部","Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others","All"]
    if user["role"] == "admin":
        region_filter = st.selectbox("按地区过滤（管理员可选）", regions_options, index=0)
    else:
        st.info(f"仅显示您所在地区的数据：{user['region']}")
        region_filter = user["region"]

    if st.button("🔍 搜索设备"):
        if not (kw or pj or sup or brand_f or (cur != "全部") or (user["role"]=="admin" and region_filter and region_filter!="全部")):
            st.warning("请输入关键词或至少一个过滤条件。")
        else:
            cond, params = [], {}
            if pj:
                cond.append("LOWER(项目名称) LIKE :pj")
                params["pj"] = f"%{pj.lower()}%"
            if sup:
                cond.append("LOWER(供应商名称) LIKE :sup")
                params["sup"] = f"%{sup.lower()}%"
            if brand_f:
                cond.append("LOWER(品牌) LIKE :brand")
                params["brand"] = f"%{brand_f.lower()}%"
            if cur != "全部":
                cond.append("币种=:c")
                params["c"] = cur
            if user["role"] != "admin":
                cond.append("地区=:r")
                params["r"] = user["region"]
            else:
                if region_filter and region_filter != "全部":
                    cond.append("地区=:r")
                    params["r"] = region_filter

            if kw:
                tokens = re.findall(r"\S+", kw)
                if search_fields:
                    fields_to_search = search_fields
                else:
                    fields_to_search = ["设备材料名称","描述","品牌","规格或型号","项目名称","供应商名称"]
                for i, token in enumerate(tokens):
                    ors = []
                    for j, f in enumerate(fields_to_search):
                        param_name = f"kw_{i}_{j}"
                        ors.append(f"LOWER({f}) LIKE :{param_name}")
                        params[param_name] = f"%{token.lower()}%"
                    cond.append("(" + " OR ".join(ors) + ")")

            sql = "SELECT * FROM quotations"
            if cond:
                sql += " WHERE " + " AND ".join(cond)
            df = pd.read_sql(sql, engine, params=params)
            safe_st_dataframe(df)
            if not df.empty:
                buf = io.BytesIO()
                with pd.ExcelWriter(buf, engine="openpyxl") as w:
                    df.to_excel(w, index=False)
                buf.seek(0)
                st.download_button("📥 下载结果", buf, "设备查询结果.xlsx")

elif page == "💰 杂费查询":
    st.header("💰 杂费查询")
    pj2 = st.text_input("按项目名称过滤")
    if st.button("🔍 搜索杂费"):
        params = {"pj": f"%{pj2.lower()}%"}
        sql = "SELECT * FROM misc_costs WHERE LOWER(项目名称) LIKE :pj"
        if user["role"] != "admin":
            sql += " AND 地区=:r"
            params["r"] = user["region"]
        df2 = pd.read_sql(sql, engine, params=params)
        safe_st_dataframe(df2)
        if not df2.empty:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as w:
                df2.to_excel(w, index=False)
            buf2.seek(0)
            st.download_button("📥 下载杂费结果", buf2, "杂费查询结果.xlsx")

elif page == "👑 管理员后台" and user["role"] == "admin":
    st.header("👑 管理员后台")
    users_df = pd.read_sql("SELECT username, role, region FROM users", engine)
    safe_st_dataframe(users_df)
