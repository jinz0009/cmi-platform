import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib, io, re, math
from datetime import date

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

# 我们期望的数据库列顺序（保持不变）
DB_COLUMNS = [
    "序号","设备材料名称","规格或型号","描述","品牌","单位","数量确认",
    "报价品牌","型号","设备单价","设备小计","人工包干单价","人工包干小计",
    "综合单价汇总","币种","原厂品牌维保期限","货期","备注",
    "询价人","项目名称","供应商名称","询价日期","录入人","地区"
]

# helper: 根据原表头尝试自动匹配到我们期望的列名（返回 None 表示无法自动匹配）
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

# 尝试从前几行检测真正的表头（支持单行或两行合并表头）
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

    # 1️⃣ Excel 批量导入（智能表头映射，支持表头不在第一行/合并单元格）
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
            st.dataframe(preview.head(10))

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

            # 构建可选映射列表（不要让用户直接映射 '录入人' 和 '地区'）
            mapping_targets = ["Ignore"] + [c for c in DB_COLUMNS if c not in ("录入人","地区")]

            # 先计算系统的自动建议（减少用户工作量）
            auto_defaults = {}
            for col in data_df.columns:
                auto_val = auto_map_header(col)
                if auto_val and auto_val in mapping_targets:
                    auto_defaults[col] = auto_val
                else:
                    auto_defaults[col] = "Ignore"

            st.markdown("系统已为每一列生成建议映射（你可以直接点击“应用映射并预览” 或 修改任意下拉再提交）。")

            # 映射表单：默认选项为 auto_defaults（用户可修改）
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
                # 构建 target -> list[source_columns] 映射
                target_sources = {}
                for src, tgt in mapped_choices.items():
                    if tgt != "Ignore":
                        target_sources.setdefault(tgt, []).append(src)

                # 1) 检测重复映射（多个源列映射到同一目标）：若存在则阻止继续
                dup_targets = {t for t, srcs in target_sources.items() if len(srcs) > 1}
                if dup_targets:
                    st.error(f"检测到重复映射：以下目标列被多个源列映射，请先调整映射（必须保证每个目标列最多对应一个源列）：{', '.join(dup_targets)}")
                    st.stop()

                # 哪些目标列没有任何源列映射
                unmapped_targets = [t for t in DB_COLUMNS if t not in ("录入人","地区") and t not in target_sources.keys()]

                # 哪些目标列被映射但对应源列在数据中全为空
                mapped_but_empty = []
                for tgt, srcs in target_sources.items():
                    has_value = False
                    for s in srcs:
                        if s in data_df.columns:
                            # 判断该源列是否有可用值（非空白/非NaN）
                            # .astype(str) 会把 NaN -> 'nan'，因此额外检查
                            col_vals = data_df[s].dropna().astype(str).str.strip()
                            if not col_vals.empty and (col_vals.str.len() > 0).any():
                                has_value = True
                                break
                    if not has_value:
                        mapped_but_empty.append(tgt)

                # 必填字段：项目名称、供应商名称、询价人、询价日期
                required_fields = ["项目名称","供应商名称","询价人","询价日期"]

                # 显示基础信息和高亮信息
                if unmapped_targets:
                    st.warning(f"未被任何源列提供值的目标列（可根据需要选择源列映射或使用右侧'默认填充'）：{', '.join(unmapped_targets)}")
                else:
                    st.success("所有目标列已被至少分配了源列（某些列可能为 Ignore 或源列为空）。")

                if mapped_but_empty:
                    st.info(f"以下目标列已映射到源列，但对应源列中似乎没有任何非空值：{', '.join(mapped_but_empty)}")

                # 2) 若必填字段缺失映射或映射列为空，必须让用户在界面上提供默认值；否则阻止导入
                st.markdown("### 必填字段（请确保这些字段在数据中存在或为其填写默认值）")
                st.markdown("如果某个必填字段已经被源列映射且存在数据，则无需填写默认值。")

                # 为必填字段显示输入（仅当该字段没有有效映射数据时，显示输入框以便用户填写默认值）
                defaults = {}
                for rf in required_fields:
                    need_input = False
                    # 如果未映射则需要输入
                    if rf not in target_sources:
                        need_input = True
                    else:
                        # 如果映射但源列为空则也需要输入默认
                        if rf in mapped_but_empty:
                            need_input = True
                    if need_input:
                        if rf == "询价日期":
                            defaults[rf] = st.date_input(f"为缺失的必填项填写默认值：{rf}", value=date.today(), key=f"default_{rf}")
                        else:
                            defaults[rf] = st.text_input(f"为缺失的必填项填写默认值：{rf}", value="", key=f"default_{rf}")
                    else:
                        defaults[rf] = None
                        st.info(f"{rf} 已由源列映射并检测到数据，无需默认填充。")

                # 检查必填字段是否满足（若仍缺失且未填默认则阻止）
                missing_required_no_default = []
                for rf in required_fields:
                    if rf not in target_sources:
                        # 未映射：必须有默认值（非空，日期除外）
                        val = defaults.get(rf)
                        if val is None or (isinstance(val, str) and val.strip() == ""):
                            missing_required_no_default.append(rf)
                    else:
                        # 已映射但映射列为空且用户未提供默认值
                        if rf in mapped_but_empty:
                            val = defaults.get(rf)
                            if val is None or (isinstance(val, str) and val.strip() == ""):
                                missing_required_no_default.append(rf)

                if missing_required_no_default:
                    st.error(f"检测到必填项未提供数据或未填写默认值：{', '.join(missing_required_no_default)}。请为这些字段输入默认值或调整映射，才可继续导入。")
                    st.stop()

                # 如果到这里则必填条件满足（要么被映射且存在数据，要么用户提供默认值）
                st.success("必填字段映射与/或默认值校验通过。您可以在下方预览并确认导入。")

                # 执行重命名并补齐缺失列以供预览/导入
                rename_dict = {orig: mapped for orig, mapped in mapped_choices.items() if mapped != "Ignore"}
                df_mapped = data_df.rename(columns=rename_dict).copy()
                for c in DB_COLUMNS:
                    if c not in df_mapped.columns:
                        df_mapped[c] = pd.NA

                # 对于用户填写的默认值，将其应用到缺失或空值的对应目标列中
                for rf in required_fields:
                    default_val = defaults.get(rf)
                    # 若用户提供了默认（非 None），则对目标列进行填充：当源列不存在或源列值为空时填充
                    if default_val is not None and (not (isinstance(default_val, str) and default_val.strip() == "")):
                        # 对日期类型做字符串化存储（与表设计一致，询价日期是 TEXT）
                        if isinstance(default_val, (date,)):
                            df_mapped[rf] = df_mapped[rf].fillna("").replace("nan", "")
                            # only fill rows where value is empty or NA
                            df_mapped.loc[df_mapped[rf].isna() | (df_mapped[rf].astype(str).str.strip() == ""), rf] = str(default_val)
                        else:
                            df_mapped[rf] = df_mapped[rf].fillna("").replace("nan", "")
                            df_mapped.loc[df_mapped[rf].isna() | (df_mapped[rf].astype(str).str.strip() == ""), rf] = str(default_val)

                # 覆盖录入人/地区为当前用户信息（保持与现有逻辑一致）
                df_mapped["录入人"] = user["username"]
                df_mapped["地区"] = user["region"]

                df_for_db = df_mapped[DB_COLUMNS]

                st.markdown("**映射后预览（前 10 行）：**")
                st.dataframe(df_for_db.head(10))

                # 导入前的最终确认（此处只有在前面的检查通过后才会到达）
                if st.button("✅ 确认并导入这些记录"):
                    try:
                        total_rows = len(df_for_db)
                        df_no_all_empty = df_for_db.dropna(how="all").reset_index(drop=True)
                        no_all_empty_rows = len(df_no_all_empty)
                        df_to_store = df_no_all_empty.drop_duplicates().reset_index(drop=True)
                        final_rows = len(df_to_store)

                        st.info(f"导入前行数: {total_rows}，去除全空行后: {no_all_empty_rows}，去重后: {final_rows}")

                        if final_rows == 0:
                            st.warning("当前没有可导入的行（0 条）。请检查映射和源数据。")
                            st.dataframe(df_for_db.head(5))
                        else:
                            st.markdown("将写入数据库的前 5 行（供确认）：")
                            st.dataframe(df_to_store.head(5))
                            try:
                                df_to_store.to_sql("quotations", engine, if_exists="append", index=False)
                                st.success(f"✅ 导入成功，共 {final_rows} 条记录已写入数据库。")
                            except Exception as e_inner:
                                st.error("写入数据库时出现异常：")
                                st.exception(e_inner)
                    except Exception as e:
                        st.error("导入流程出现异常：")
                        st.exception(e)

    # 2️⃣ 手工录入（保持不变）
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
        date_input = st.date_input("询价日期")
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
                        "d": desc,"r": remark,"dt": str(date_input),"u": user["username"],"reg": user["region"]})
            st.success("✅ 已添加记录。")

    # 3️⃣ 杂费录入（保持不变）
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
            st.dataframe(df)
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
        st.dataframe(df2)
        if not df2.empty:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as w:
                df2.to_excel(w, index=False)
            buf2.seek(0)
            st.download_button("📥 下载杂费结果", buf2, "杂费查询结果.xlsx")

elif page == "👑 管理员后台" and user["role"] == "admin":
    st.header("👑 管理员后台")
    users_df = pd.read_sql("SELECT username, role, region FROM users", engine)
    st.dataframe(users_df)
