# -*- coding: utf-8 -*-
"""
Complete app_streamlit.py — bilingual (ZH/EN) with top-right language switch.
Save and run:
    streamlit run main/app_streamlit.py
"""
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import hashlib, io, re, pathlib
from datetime import date

# ------------------ Page config (static) ------------------
st.set_page_config(page_title="CMI Portal", layout="wide")


# ================== i18n (ZH/EN) ==================
if "lang" not in st.session_state:
    st.session_state.lang = "zh"

def set_lang(l: str):
    st.session_state.lang = l

I18N = {
    "zh": {
        # top
        "btn_zh": "中文",
        "btn_en": "EN",

        # auth
        "tab_login": "🔑 登录",
        "tab_register": "🧾 注册",
        "login_title": "🔐 用户登录",
        "register_title": "🧾 注册",
        "username": "用户名",
        "password": "密码",
        "new_username": "新用户名",
        "new_password": "新密码",
        "region": "地区",
        "btn_login": "登录",
        "btn_register": "注册",
        "btn_logout": "退出登录",
        "err_need_user_pass": "请输入用户名和密码",
        "err_wrong_cred": "用户名或密码错误",
        "warn_empty_user_pass": "用户名和密码不能为空",
        "ok_register": "注册成功，请登录",
        "err_user_exists": "用户名已存在",

        # sidebar / nav
        "sidebar_user": "👤 用户",
        "sidebar_region": "🏢 地区",
        "sidebar_role": "🔑 角色",
        "nav": "导航",
        "only_show_region": "仅显示您所在地区的数据：{region}",

        # pages (display labels)
        "page_entry": "🏠 录入页面",
        "page_device": "📋 设备查询",
        "page_misc": "💰 杂费查询",
        "page_admin": "👑 管理员后台",

        # entry page
        "app_title": "📊 询价录入与查询平台",
        "excel_import": "📂 Excel 批量录入",
        "excel_hint": "系统会尝试识别上传文件的表头并给出建议映射。",
        "btn_download_template": "下载模板",
        "upload_excel": "上传 Excel (.xlsx)",
        "read_preview_fail": "读取预览失败：{e}",
        "raw_headers": "**检测到的原始表头（用于映射，系统已尝试自动对应一版建议）：**",
        "mapping_hint": "系统已为每一列生成建议映射（你可以直接点击“应用映射并预览” 或 修改任意下拉再提交）。",
        "btn_apply_mapping": "应用映射并预览",
        "mapping_saved": "映射已保存。现在请填写全局必填信息并提交以继续校验与导入。",

        "mapped_preview": "**映射后预览（前 10 行）：**",
        "mapped_preview_fail": "映射数据无法预览，请重新映射。",
        "btn_open_global": "➡️ 填写/查看全局信息并应用导入",
        "global_tip": "（若需要对空值进行统一填充，例如币种/项目/供应商/询价人，请展开并填写全局信息）",
        "global_fill_title": "请填写全局必填信息（仅填充空值）。填写完后点击“应用全局并继续校验”：",
        "project": "项目名称",
        "supplier": "供应商名称",
        "enquirer": "询价人",
        "inq_date": "询价日期",
        "currency_fill": "币种（用于填充空值）",
        "btn_apply_global": "应用全局并继续校验",
        "err_global_required": "必须填写：项目名称、供应商名称、询价人和询价日期",
        "err_need_currency": "由于源数据存在空的币种，请选择币种以继续。",
        "ok_global_applied": "已应用全局信息，正在进行总体必填校验...",

        "import_ok_all": "✅ 已导入全部 {n} 条有效记录。",
        "import_ok_part": "✅ 已导入 {n} 条有效记录（跳过 {m} 条）。",
        "import_none_valid": "没有找到满足总体必填条件的记录可导入。",
        "import_err": "导入有效记录时发生错误：{e}",
        "invalid_rows_warn": "以下 {n} 条记录缺少总体必填字段，未被导入，请修正后重新导入：",
        "btn_download_invalid": "📥 下载未通过记录（用于修正）",
        "mapping_saved_info": "映射保存。请填写全局信息（若必要）并应用以继续导入。",

        # manual entry
        "manual_device": "✏️ 手工录入设备",
        "device_name": "设备材料名称",
        "brand_optional": "品牌（可选）",
        "qty": "数量确认",
        "device_price": "设备单价",
        "labor_price": "人工包干单价",
        "currency": "币种",
        "desc": "描述",
        "btn_add_manual": "添加记录（手动）",
        "err_manual_required": "必填项不能为空：项目名称、供应商名称、询价人、设备材料名称",
        "err_need_price_one": "请至少填写 设备单价 或 人工包干单价 中的一项（两者至少填一项）。",
        "ok_manual_added": "手工记录已添加（品牌为可选）。",
        "err_manual_add": "添加记录失败：{e}",

        # misc manual
        "manual_misc": "💰 手工录入杂费",
        "misc_cat": "杂费类目（例如运输/安装/税费）",
        "amount": "金额",
        "note_optional": "备注（可选）",
        "occ_date": "发生日期",
        "btn_add_misc": "添加杂费记录",
        "err_misc_required": "请填写项目名称、杂费类目和金额",
        "ok_misc_added": "✅ 杂费记录已添加",
        "err_misc_add": "添加杂费记录失败：{e}",

        # device search
        "device_search": "📋 设备查询",
        "kw": "关键词（多个空格分词）",
        "search_fields": "搜索字段（留空为默认）",
        "filter_project": "按项目名称过滤",
        "filter_supplier": "按供应商名称过滤",
        "filter_brand": "按品牌过滤",
        "filter_currency": "币种",
        "filter_region_admin": "按地区过滤（管理员）",
        "btn_search_device": "🔍 搜索设备",
        "query_fail": "查询失败：{e}",
        "no_records": "未找到符合条件的记录。",
        "btn_download_result": "下载结果",
        "price_overview": "### 当前查询 — 价格统计概览（基于返回记录）",
        "m_dev_mean": "设备单价 — 均价",
        "m_dev_min": "设备单价 — 最低价",
        "m_lab_mean": "人工包干单价 — 均价",
        "m_lab_min": "人工包干单价 — 最低价",
        "dev_min_rows": "#### 设备单价 — 历史最低价对应记录（可能多条并列）",
        "lab_min_rows": "#### 人工包干单价 — 历史最低价对应记录（可能多条并列）",
        "no_dev_price": "查询结果中无有效的设备单价，无法显示最低设备单价对应记录。",
        "no_lab_price": "查询结果中无有效的人工包干单价，无法显示最低人工单价对应记录。",
        "group_stats": "#### 按设备名称分组 — 均价 / 最低价",
        "price_stat_warn": "计算和展示价格统计/最低价对应记录时发生异常：{e}",

        # admin delete quotations
        "admin_delete_title": "⚠️ 管理员删除：选择记录并确认。",
        "select_delete": "选中要删除的记录",
        "confirm_delete": "我确认删除所选记录（不可恢复）",
        "btn_delete_admin": "删除所选记录（管理员）",
        "warn_select_first": "请先选择要删除的记录。",
        "warn_confirm_first": "请勾选确认框以执行删除。",
        "parse_rowid_fail": "解析所选 rowid 失败：{e}",
        "no_valid_rowid": "无有效 rowid，取消删除。",
        "delete_req_rowids": "请求删除的 rowid 列表：",
        "matched_to_delete": "匹配到以下记录（将在确认后删除）：",
        "no_match_cancel": "数据库中未匹配到任何所选 rowid，取消删除。",
        "no_deletable_stop": "无可删除记录，停止。",
        "archive_done": "归档完成（如失败会在下方显示异常）。",
        "archive_fail": "归档尝试失败（已记录但不阻止删除）：{e}",
        "delete_sql_done": "DELETE SQL 已执行：",
        "delete_rowcount": "数据库返回的 rowcount：",
        "delete_exec_fail": "执行 DELETE 时异常：{e}",
        "after_check_ok": "删除后复查：这些 rowid 已不存在（删除成功）。",
        "after_check_warn": "删除后复查：部分或全部记录仍存在（删除未生效或被恢复）：",
        "after_check_fail": "删除后复核查询失败：{e}",
        "not_admin_delete": "仅管理员可删除记录。",

        # misc search
        "misc_search": "💰 杂费查询",
        "btn_search_misc": "🔍 搜索杂费",
        "btn_download_misc": "下载杂费结果",

        # admin user mgmt
        "admin_console": "👑 管理员后台 — 用户管理",
        "update_region_title": "🛠️ 修改用户地区（Region）",
        "select_user": "选择要修改的用户",
        "new_region": "新地区",
        "confirm_update": "我确认要修改该用户地区",
        "btn_update_region": "更新地区",
        "user_not_found": "未找到该用户（可能已被删除），请刷新页面。",
        "protect_admin": "系统默认 admin 不建议修改地区。如确需修改，请先新增一个管理员账号再操作。",
        "need_confirm_update": "请勾选确认框后再更新。",
        "update_ok": "✅ 已更新用户 {u} 的地区为：{r}",
        "update_fail": "更新失败：{e}",

        "delete_user_title": "🗑️ 删除用户账号",
        "delete_user_caption": "说明：删除账号不会自动删除该用户已录入的报价/杂费数据（这些记录仍会保留在 quotations / misc_costs 表中）。",
        "no_deletable_users": "当前没有可删除的用户（已保护当前登录用户与默认 admin）。",
        "select_users_to_delete": "选择要删除的用户（可多选）",
        "confirm_delete_users": "我确认删除所选用户（不可恢复）",
        "btn_delete_users": "删除用户",
        "reject_protected": "所选用户包含受保护账号（当前登录用户或默认 admin），已拒绝删除。",
        "delete_users_ok": "✅ 已删除 {n} 个用户账号",
        "delete_users_fail": "删除失败：{e}",

        # refresh
        "manual_refresh": "手动刷新页面",
    },
    "en": {
        # top
        "btn_zh": "中文",
        "btn_en": "EN",

        # auth
        "tab_login": "🔑 Sign in",
        "tab_register": "🧾 Register",
        "login_title": "🔐 Sign in",
        "register_title": "🧾 Register",
        "username": "Username",
        "password": "Password",
        "new_username": "New username",
        "new_password": "New password",
        "region": "Region",
        "btn_login": "Sign in",
        "btn_register": "Register",
        "btn_logout": "Sign out",
        "err_need_user_pass": "Please enter username and password.",
        "err_wrong_cred": "Incorrect username or password.",
        "warn_empty_user_pass": "Username and password cannot be empty.",
        "ok_register": "Registered successfully. Please sign in.",
        "err_user_exists": "Username already exists.",

        # sidebar / nav
        "sidebar_user": "👤 User",
        "sidebar_region": "🏢 Region",
        "sidebar_role": "🔑 Role",
        "nav": "Navigation",
        "only_show_region": "Only showing data for your region: {region}",

        # pages (display labels)
        "page_entry": "🏠 Entry",
        "page_device": "📋 Device Search",
        "page_misc": "💰 Misc Cost Search",
        "page_admin": "👑 Admin Console",

        # entry page
        "app_title": "📊 Quotation Entry & Search Portal",
        "excel_import": "📂 Bulk Import (Excel)",
        "excel_hint": "The system will try to detect headers and suggest a mapping.",
        "btn_download_template": "Download template",
        "upload_excel": "Upload Excel (.xlsx)",
        "read_preview_fail": "Failed to read preview: {e}",
        "raw_headers": "**Detected raw headers (used for mapping; auto-suggestions are applied):**",
        "mapping_hint": "Suggested mapping is generated for each column. Click “Apply mapping & preview”, or adjust dropdowns and submit.",
        "btn_apply_mapping": "Apply mapping & preview",
        "mapping_saved": "Mapping saved. Please fill global required fields and submit to validate & import.",

        "mapped_preview": "**Mapped preview (first 10 rows):**",
        "mapped_preview_fail": "Preview unavailable. Please redo mapping.",
        "btn_open_global": "➡️ Open global fields & import",
        "global_tip": "(Use global fill to populate empty cells like currency/project/supplier/enquirer.)",
        "global_fill_title": "Fill global required fields (only fills empty cells). Then click “Apply & validate”:",
        "project": "Project",
        "supplier": "Supplier",
        "enquirer": "Enquirer",
        "inq_date": "Enquiry date",
        "currency_fill": "Currency (fill empty cells)",
        "btn_apply_global": "Apply & validate",
        "err_global_required": "Required: Project, Supplier, Enquirer, and Enquiry date.",
        "err_need_currency": "Some rows have empty currency. Please select a currency to continue.",
        "ok_global_applied": "Global fields applied. Validating required fields...",

        "import_ok_all": "✅ Imported {n} valid rows.",
        "import_ok_part": "✅ Imported {n} valid rows (skipped {m}).",
        "import_none_valid": "No rows passed the required-field validation.",
        "import_err": "Error importing valid rows: {e}",
        "invalid_rows_warn": "{n} rows failed validation and were not imported. Please fix and re-import:",
        "btn_download_invalid": "📥 Download invalid rows",
        "mapping_saved_info": "Mapping saved. Please apply global fields (if needed) to continue import.",

        # manual entry
        "manual_device": "✏️ Manual device entry",
        "device_name": "Device/Material name",
        "brand_optional": "Brand (optional)",
        "qty": "Quantity confirmed",
        "device_price": "Device unit price",
        "labor_price": "Labor unit price",
        "currency": "Currency",
        "desc": "Description",
        "btn_add_manual": "Add record (manual)",
        "err_manual_required": "Required fields cannot be empty: Project, Supplier, Enquirer, Device/Material name.",
        "err_need_price_one": "Please fill at least one: Device unit price OR Labor unit price.",
        "ok_manual_added": "Manual record added (brand is optional).",
        "err_manual_add": "Failed to add record: {e}",

        # misc manual
        "manual_misc": "💰 Manual misc cost entry",
        "misc_cat": "Category (e.g., shipping/installation/tax)",
        "amount": "Amount",
        "note_optional": "Note (optional)",
        "occ_date": "Occurrence date",
        "btn_add_misc": "Add misc cost",
        "err_misc_required": "Please fill Project, Category, and Amount.",
        "ok_misc_added": "✅ Misc cost added",
        "err_misc_add": "Failed to add misc cost: {e}",

        # device search
        "device_search": "📋 Device Search",
        "kw": "Keywords (split by spaces)",
        "search_fields": "Search fields (empty = default)",
        "filter_project": "Filter by project",
        "filter_supplier": "Filter by supplier",
        "filter_brand": "Filter by brand",
        "filter_currency": "Currency",
        "filter_region_admin": "Filter by region (Admin)",
        "btn_search_device": "🔍 Search devices",
        "query_fail": "Query failed: {e}",
        "no_records": "No records found.",
        "btn_download_result": "Download results",
        "price_overview": "### Price overview (based on returned records)",
        "m_dev_mean": "Device price — Average",
        "m_dev_min": "Device price — Minimum",
        "m_lab_mean": "Labor price — Average",
        "m_lab_min": "Labor price — Minimum",
        "dev_min_rows": "#### Minimum device price — matching rows (may be multiple ties)",
        "lab_min_rows": "#### Minimum labor price — matching rows (may be multiple ties)",
        "no_dev_price": "No valid device price in results; cannot display minimum-price rows.",
        "no_lab_price": "No valid labor price in results; cannot display minimum-price rows.",
        "group_stats": "#### Grouped by device name — avg / min",
        "price_stat_warn": "Error computing / rendering stats: {e}",

        # admin delete quotations
        "admin_delete_title": "⚠️ Admin delete: select rows and confirm.",
        "select_delete": "Select records to delete",
        "confirm_delete": "I confirm deletion (irreversible)",
        "btn_delete_admin": "Delete selected (Admin)",
        "warn_select_first": "Please select records first.",
        "warn_confirm_first": "Please tick the confirmation checkbox.",
        "parse_rowid_fail": "Failed to parse rowid: {e}",
        "no_valid_rowid": "No valid rowid. Deletion cancelled.",
        "delete_req_rowids": "Rowids requested for deletion:",
        "matched_to_delete": "Matched rows (will be deleted after confirmation):",
        "no_match_cancel": "No row matched in DB. Deletion cancelled.",
        "no_deletable_stop": "Nothing to delete. Stop.",
        "archive_done": "Archive done (errors, if any, are shown below).",
        "archive_fail": "Archive attempt failed (will not block deletion): {e}",
        "delete_sql_done": "DELETE SQL executed:",
        "delete_rowcount": "Rowcount returned:",
        "delete_exec_fail": "DELETE failed: {e}",
        "after_check_ok": "Post-check: rowids no longer exist (deleted).",
        "after_check_warn": "Post-check: some rows still exist (delete not effective):",
        "after_check_fail": "Post-check query failed: {e}",
        "not_admin_delete": "Only admin can delete records.",

        # misc search
        "misc_search": "💰 Misc Cost Search",
        "btn_search_misc": "🔍 Search misc costs",
        "btn_download_misc": "Download misc results",

        # admin user mgmt
        "admin_console": "👑 Admin Console — User Management",
        "update_region_title": "🛠️ Update user region",
        "select_user": "Select user",
        "new_region": "New region",
        "confirm_update": "I confirm updating this user's region",
        "btn_update_region": "Update region",
        "user_not_found": "User not found (may have been deleted). Please refresh.",
        "protect_admin": "Default admin is protected. Create another admin if you must change it.",
        "need_confirm_update": "Please tick the confirmation checkbox first.",
        "update_ok": "✅ Updated {u}'s region to: {r}",
        "update_fail": "Update failed: {e}",

        "delete_user_title": "🗑️ Delete user accounts",
        "delete_user_caption": "Note: deleting users will NOT delete their quotation/misc data (records remain in DB tables).",
        "no_deletable_users": "No deletable users (current user and default admin are protected).",
        "select_users_to_delete": "Select users to delete (multi-select)",
        "confirm_delete_users": "I confirm deletion (irreversible)",
        "btn_delete_users": "Delete users",
        "reject_protected": "Selection includes protected accounts (current user or default admin). Deletion rejected.",
        "delete_users_ok": "✅ Deleted {n} users",
        "delete_users_fail": "Delete failed: {e}",

        # refresh
        "manual_refresh": "Refresh page",
    }
}

def t(key: str, **kwargs) -> str:
    lang = st.session_state.get("lang", "zh")
    s = I18N.get(lang, I18N["zh"]).get(key, key)
    try:
        return s.format(**kwargs)
    except Exception:
        return s


# ------------------ Top-right language switch (works on every page) ------------------
top_left, top_mid, top_right = st.columns([10, 1, 1], vertical_alignment="center")
with top_mid:
    st.button(t("btn_zh"), use_container_width=True, on_click=set_lang, args=("zh",))
with top_right:
    st.button(t("btn_en"), use_container_width=True, on_click=set_lang, args=("en",))


# --- Compatibility helper: safe_rerun ---
def safe_rerun():
    st.rerun()


# Database engine (adjust URI for production)
engine = create_engine("sqlite:///quotation.db", connect_args={"check_same_thread": False})

# Debug: show DB path (optional)
# st.write("DB path:", pathlib.Path(engine.url.database).absolute())

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
        地区 TEXT,
        发生日期 TEXT
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
    st.subheader(t("login_title"))

    with st.form("login_form"):
        u = st.text_input(t("username"))
        p = st.text_input(t("password"), type="password")
        submitted = st.form_submit_button(t("btn_login"))

    if submitted:
        if not u or not p:
            st.error(t("err_need_user_pass"))
            return

        pw_hash = hashlib.sha256(p.encode()).hexdigest()

        with engine.begin() as conn:
            user = conn.execute(
                text("SELECT username, role, region FROM users WHERE username=:u AND password=:p"),
                {"u": u, "p": pw_hash}
            ).fetchone()

        if user:
            st.session_state["user"] = {
                "username": user.username,
                "role": user.role,
                "region": user.region
            }
            st.rerun()
        else:
            st.error(t("err_wrong_cred"))

def register_form():
    st.subheader(t("register_title"))
    with st.form("register_form", clear_on_submit=False):
        ru = st.text_input(t("new_username"), key="reg_user")
        rp = st.text_input(t("new_password"), type="password", key="reg_pass")
        region = st.selectbox(t("region"), ["Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others"])
        submitted = st.form_submit_button(t("btn_register"))

    if submitted:
        if not ru or not rp:
            st.warning(t("warn_empty_user_pass"))
            return
        pw_hash = hashlib.sha256(rp.encode()).hexdigest()
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("INSERT INTO users (username,password,role,region) VALUES (:u,:p,'user',:r)"),
                    {"u": ru, "p": pw_hash, "r": region}
                )
            st.success(t("ok_register"))
        except Exception:
            st.error(t("err_user_exists"))

def logout():
    if "user" in st.session_state:
        del st.session_state["user"]
    safe_rerun()


# ============ Page flow ============
if "user" not in st.session_state:
    tabs = st.tabs([t("tab_login"), t("tab_register")])
    with tabs[0]:
        login_form()
    with tabs[1]:
        register_form()
    st.stop()

if st.session_state.get("_needs_refresh", False):
    if st.button(t("manual_refresh"), key="manual_refresh"):
        safe_rerun()

user = st.session_state["user"]

st.sidebar.markdown(
    f"{t('sidebar_user')}: **{user['username']}**  \n"
    f"{t('sidebar_region')}: {user['region']}  \n"
    f"{t('sidebar_role')}: {user['role']}"
)

if st.sidebar.button(t("btn_logout"), key="logout_btn"):
    logout()

PAGE_KEYS_ADMIN = ["entry", "device", "misc", "admin"]
PAGE_KEYS_USER  = ["entry", "device", "misc"]

page = st.sidebar.radio(
    t("nav"),
    PAGE_KEYS_ADMIN if user["role"] == "admin" else PAGE_KEYS_USER,
    format_func=lambda k: {
        "entry": t("page_entry"),
        "device": t("page_device"),
        "misc": t("page_misc"),
        "admin": t("page_admin"),
    }[k],
)


# ============ Main: Upload / Mapping / Import ============
if page == "entry":
    st.title(t("app_title"))
    st.header(t("excel_import"))
    st.caption(t("excel_hint"))

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人","地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template.to_excel(writer, index=False)
    buf.seek(0)
    st.download_button(t("btn_download_template"), buf, "quotation_template.xlsx", key="download_template")

    uploaded = st.file_uploader(t("upload_excel"), type=["xlsx"], key="upload_excel")
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
            st.error(t("read_preview_fail", e=e))
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

            st.markdown(t("raw_headers"))
            st.write(list(data_df.columns))

            mapping_targets = ["Ignore"] + [c for c in DB_COLUMNS if c not in ("录入人","地区")]

            auto_defaults = {}
            for col in data_df.columns:
                auto_val = auto_map_header(col)
                if auto_val and auto_val in mapping_targets:
                    auto_defaults[col] = auto_val
                else:
                    auto_defaults[col] = "Ignore"

            st.markdown(t("mapping_hint"))

            mapped_choices = {}
            with st.form("mapping_form", clear_on_submit=False):
                cols_left, cols_right = st.columns(2)
                for i, col in enumerate(data_df.columns):
                    default = auto_defaults.get(col, "Ignore")
                    container = cols_left if i % 2 == 0 else cols_right
                    sel = container.selectbox(
                        f"Source column: {col}" if st.session_state.lang == "en" else f"源列: {col}",
                        mapping_targets,
                        index=mapping_targets.index(default) if default in mapping_targets else 0,
                        key=f"map_{i}"
                    )
                    mapped_choices[col] = sel
                submitted = st.form_submit_button(t("btn_apply_mapping"))

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
                                ser_norm = ser.astype(str).map(
                                    lambda x: None if x is None else (str(x).strip() if str(x).strip().lower() not in ("", "nan", "none") else pd.NA)
                                )

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

                st.success(t("mapping_saved"))

    # ====== 映射后预览 + “填写全局信息并导入” 流程 ======
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
            st.error(f"Restore mapped data failed: {e}" if st.session_state.lang == "en" else f"恢复映射数据失败：{e}")
            df_for_db = None

        st.markdown(t("mapped_preview"))
        if df_for_db is not None:
            safe_st_dataframe(df_for_db.head(10))
        else:
            st.info(t("mapped_preview_fail"))

        if "show_global_form" not in st.session_state:
            st.session_state["show_global_form"] = False

        col_show, col_hint = st.columns([1, 6])
        if col_show.button(t("btn_open_global"), key="open_global_form_btn"):
            st.session_state["show_global_form"] = True
        col_hint.markdown(t("global_tip"))

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

            st.markdown(t("global_fill_title"))
            with st.form("global_form_v2"):
                g1, g2, g3, g4, g5 = st.columns(5)
                g_project = g1.text_input(t("project"), key="global_project_input", value=st.session_state["bulk_values"].get("project", ""))
                g_supplier = g2.text_input(t("supplier"), key="global_supplier_input", value=st.session_state["bulk_values"].get("supplier", ""))
                g_enquirer = g3.text_input(t("enquirer"), key="global_enquirer_input", value=st.session_state["bulk_values"].get("enquirer", ""))
                default_date = st.session_state["bulk_values"].get("date", "")
                try:
                    if default_date:
                        g_date = g4.date_input(t("inq_date"), value=pd.to_datetime(default_date).date(), key="global_date_input")
                    else:
                        g_date = g4.date_input(t("inq_date"), value=date.today(), key="global_date_input")
                except Exception:
                    g_date = g4.date_input(t("inq_date"), value=date.today(), key="global_date_input")

                g_currency = None
                if need_global_currency:
                    currency_options = ["", "IDR", "USD", "RMB", "SGD", "MYR", "THB"]
                    curr_default = st.session_state["bulk_values"].get("currency", "")
                    default_idx = currency_options.index(curr_default) if curr_default in currency_options else 0
                    g_currency = g5.selectbox(t("currency_fill"), currency_options, index=default_idx, key="global_currency_input")
                else:
                    g5.write("")

                apply_global = st.form_submit_button(t("btn_apply_global"))

            if apply_global:
                if not (g_project and g_supplier and g_enquirer and g_date):
                    st.error(t("err_global_required"))
                    st.session_state["bulk_applied"] = False
                elif need_global_currency and (g_currency is None or str(g_currency).strip() == ""):
                    st.error(t("err_need_currency"))
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
                    st.success(t("ok_global_applied"))

            if st.session_state.get("bulk_applied", False):
                try:
                    csv_buf2 = io.StringIO(st.session_state["mapping_csv"])
                    df_for_db = pd.read_csv(csv_buf2, dtype=object)
                    for c in DB_COLUMNS:
                        if c not in df_for_db.columns:
                            df_for_db[c] = pd.NA
                    df_for_db = df_for_db[DB_COLUMNS]
                except Exception as e:
                    st.error(f"Restore mapped data failed: {e}" if st.session_state.lang == "en" else f"恢复映射数据失败：{e}")
                    df_for_db = None

                if df_for_db is None:
                    st.error("Mapped data missing; cannot import." if st.session_state.lang == "en" else "映射数据丢失，无法继续导入。")
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

                    # --- Validation: brand NOT required; price rule: either 设备单价 or 人工包干单价 must be present
                    def normalize_cell(x):
                        if pd.isna(x):
                            return None
                        s = str(x).strip()
                        if s.lower() in ("", "nan", "none"):
                            return None
                        return s

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
                            final_check = df_to_store[["设备材料名称","设备单价","人工包干单价","币种"]].applymap(normalize_cell)

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
                                    st.success(t("import_ok_part", n=imported_count, m=len(still_bad)))
                                else:
                                    st.info("No valid rows to import (all candidates failed final check)." if st.session_state.lang == "en" else "没有可导入的有效记录（所有候选在最终检查中被判为不完整）。")
                                if not still_bad.empty:
                                    df_invalid = pd.concat([df_invalid, still_bad], ignore_index=True)
                            else:
                                with engine.begin() as conn:
                                    df_to_store.to_sql("quotations", conn, if_exists="append", index=False)
                                imported_count = len(df_to_store)
                                st.success(t("import_ok_all", n=imported_count))
                        except Exception as e:
                            st.error(t("import_err", e=e))
                    else:
                        st.info(t("import_none_valid"))

                    if not df_invalid.empty:
                        st.warning(t("invalid_rows_warn", n=len(df_invalid)))
                        safe_st_dataframe(df_invalid.head(50))
                        buf_bad = io.BytesIO()
                        with pd.ExcelWriter(buf_bad, engine="openpyxl") as w:
                            df_invalid.to_excel(w, index=False)
                        buf_bad.seek(0)
                        st.download_button(t("btn_download_invalid"), buf_bad, "invalid_rows.xlsx")

                    st.session_state["bulk_applied"] = False
    else:
        st.info(t("mapping_saved_info"))

    # ------------------ 手工录入（品牌不必填） ------------------
    st.header(t("manual_device"))
    with st.form("manual_add_form_original", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input(t("project"), key="manual_project_orig")
        sup = col2.text_input(t("supplier"), key="manual_supplier_orig")
        inq = col3.text_input(t("enquirer"), key="manual_enquirer_orig")
        name = st.text_input(t("device_name"), key="manual_name_orig")
        brand = st.text_input(t("brand_optional"), key="manual_brand_orig")
        qty = st.number_input(t("qty"), min_value=0.0, key="manual_qty_orig")
        price = st.number_input(t("device_price"), min_value=0.0, key="manual_price_orig")
        labor_price = st.number_input(t("labor_price"), min_value=0.0, key="manual_labor_price_orig")
        cur = st.selectbox(t("currency"), ["IDR","USD","RMB","SGD","MYR","THB"], key="manual_currency_orig")
        desc = st.text_area(t("desc"), key="manual_desc_orig")
        date_inq = st.date_input(t("inq_date"), value=date.today(), key="manual_date_orig")
        submit_manual = st.form_submit_button(t("btn_add_manual"), key="manual_submit_orig")

    if submit_manual:
        if not (pj and sup and inq and name):
            st.error(t("err_manual_required"))
        else:
            def has_price_value(v):
                if pd.isna(v):
                    return False
                s = str(v).strip()
                if s == "" or s.lower() in ("nan","none"):
                    return False
                return True

            if not (has_price_value(price) or has_price_value(labor_price)):
                st.error(t("err_need_price_one"))
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
                    st.success(t("ok_manual_added"))
                except Exception as e:
                    st.error(t("err_manual_add", e=e))

    # ------------------ 杂费手工录入（含发生日期列） ------------------
    st.header(t("manual_misc"))
    with st.form("manual_misc_form", clear_on_submit=True):
        mcol1, mcol2, mcol3 = st.columns(3)
        misc_project = mcol1.text_input(t("project"), key="misc_project_input")
        misc_category = mcol2.text_input(t("misc_cat"), key="misc_category_input")
        misc_amount = mcol3.number_input(t("amount"), min_value=0.0, format="%f", key="misc_amount_input")
        mc1, mc2 = st.columns(2)
        misc_currency = mc1.selectbox(t("currency"), ["IDR","USD","RMB","SGD","MYR","THB"], key="misc_currency_input")
        misc_note = mc2.text_input(t("note_optional"), key="misc_note_input")
        misc_date = st.date_input(t("occ_date"), value=date.today(), key="misc_date_input")
        submit_misc = st.form_submit_button(t("btn_add_misc"))

    if submit_misc:
        if not (misc_project and misc_category) or misc_amount is None:
            st.error(t("err_misc_required"))
        else:
            try:
                with engine.begin() as conn:
                    conn.execute(text("""
                        INSERT INTO misc_costs (项目名称, 杂费类目, 金额, 币种, 录入人, 地区, 发生日期)
                        VALUES (:pj, :cat, :amt, :cur, :user, :region, :occ_date)
                    """), {
                        "pj": misc_project,
                        "cat": misc_category,
                        "amt": float(misc_amount),
                        "cur": misc_currency,
                        "user": user["username"],
                        "region": user["region"],
                        "occ_date": str(misc_date)
                    })
                st.success(t("ok_misc_added"))
            except Exception as e:
                st.error(t("err_misc_add", e=e))


# ============ Search / Delete (Admin) ============
if page == "device":
    st.header(t("device_search"))
    kw = st.text_input(t("kw"), key="search_kw")
    search_fields = st.multiselect(
        t("search_fields"),
        ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称", "地区"],
        key="search_fields"
    )
    pj_filter = st.text_input(t("filter_project"), key="search_pj")
    sup_filter = st.text_input(t("filter_supplier"), key="search_sup")
    brand_filter = st.text_input(t("filter_brand"), key="search_brand")
    cur_filter = st.selectbox(t("filter_currency"), ["全部","IDR","USD","RMB","SGD","MYR","THB"], index=0, key="search_cur")

    regions_options = ["全部","Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others","All"]
    if user["role"] == "admin":
        region_filter = st.selectbox(t("filter_region_admin"), regions_options, index=0, key="search_region")
    else:
        st.info(t("only_show_region", region=user["region"]))
        region_filter = user["region"]

    if st.button(t("btn_search_device"), key="search_button"):
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
            for i, tok in enumerate(tokens):
                ors = []
                for j, f in enumerate(fields):
                    pname = f"kw_{i}_{j}"
                    ors.append(f"LOWER({f}) LIKE :{pname}")
                    params[pname] = f"%{tok.lower()}%"
                conds.append("(" + " OR ".join(ors) + ")")

        sql = "SELECT rowid, * FROM quotations"
        if conds:
            sql += " WHERE " + " AND ".join(conds)

        try:
            df = pd.read_sql(sql, engine, params=params)
        except Exception as e:
            st.error(t("query_fail", e=e))
            df = pd.DataFrame()

        if df.empty:
            st.info(t("no_records"))
        else:
            safe_st_dataframe(df)
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            buf.seek(0)
            st.download_button(t("btn_download_result"), buf, "device_search_results.xlsx" if st.session_state.lang == "en" else "设备查询结果.xlsx", key="download_search")

            # --- Price statistics + full-row lowest-price display ---
            try:
                df_prices = df.copy()
                device_price_col = "设备单价"
                labor_price_col = "人工包干单价"
                name_col = "设备材料名称"

                if device_price_col in df_prices.columns:
                    df_prices[device_price_col] = pd.to_numeric(df_prices[device_price_col], errors="coerce")
                else:
                    df_prices[device_price_col] = pd.Series([pd.NA] * len(df_prices))

                if labor_price_col in df_prices.columns:
                    df_prices[labor_price_col] = pd.to_numeric(df_prices[labor_price_col], errors="coerce")
                else:
                    df_prices[labor_price_col] = pd.Series([pd.NA] * len(df_prices))

                overall = {
                    "dev_mean": df_prices[device_price_col].mean(skipna=True),
                    "dev_min": df_prices[device_price_col].min(skipna=True),
                    "lab_mean": df_prices[labor_price_col].mean(skipna=True),
                    "lab_min": df_prices[labor_price_col].min(skipna=True),
                }

                def fmt(v):
                    return "-" if (v is None or (isinstance(v, float) and pd.isna(v))) else (f"{v:,.2f}" if isinstance(v, (int, float)) else str(v))

                st.markdown(t("price_overview"))
                c1, c2, c3, c4 = st.columns(4)
                c1.metric(t("m_dev_mean"), fmt(overall["dev_mean"]))
                c2.metric(t("m_dev_min"), fmt(overall["dev_min"]))
                c3.metric(t("m_lab_mean"), fmt(overall["lab_mean"]))
                c4.metric(t("m_lab_min"), fmt(overall["lab_min"]))

                if not pd.isna(overall["dev_min"]):
                    dev_min_val = overall["dev_min"]
                    dev_min_rows = df_prices[df_prices[device_price_col] == dev_min_val].copy()
                    st.markdown(t("dev_min_rows"))
                    safe_st_dataframe(dev_min_rows.reset_index(drop=True))
                else:
                    st.info(t("no_dev_price"))

                if not pd.isna(overall["lab_min"]):
                    lab_min_val = overall["lab_min"]
                    lab_min_rows = df_prices[df_prices[labor_price_col] == lab_min_val].copy()
                    st.markdown(t("lab_min_rows"))
                    safe_st_dataframe(lab_min_rows.reset_index(drop=True))
                else:
                    st.info(t("no_lab_price"))

                if name_col in df_prices.columns:
                    agg = df_prices.groupby(name_col).agg(
                        设备单价_均价=(device_price_col, lambda s: s.mean(skipna=True)),
                        设备单价_最低=(device_price_col, lambda s: s.min(skipna=True)),
                        人工包干单价_均价=(labor_price_col, lambda s: s.mean(skipna=True)),
                        人工包干单价_最低=(labor_price_col, lambda s: s.min(skipna=True)),
                        样本数=(device_price_col, "count")
                    ).reset_index()
                    st.markdown(t("group_stats"))
                    safe_st_dataframe(agg.sort_values(by="设备单价_均价", ascending=True).head(200))
            except Exception as e:
                st.warning(t("price_stat_warn", e=e))

            # Admin delete form (single form)
            if user["role"] == "admin":
                st.markdown("---")
                st.markdown(t("admin_delete_title"))
                choices = []
                for _, row in df.iterrows():
                    rid = int(row["rowid"])
                    proj = str(row.get("项目名称",""))[:40]
                    name = str(row.get("设备材料名称",""))[:60]
                    brand = str(row.get("品牌",""))[:30]
                    choices.append(f"{rid} | {proj} | {name} | {brand}")

                with st.form("admin_delete_form_final_v2", clear_on_submit=False):
                    selected = st.multiselect(t("select_delete"), choices, key="admin_delete_selected_v2")
                    confirm = st.checkbox(t("confirm_delete"), key="admin_delete_confirm_v2")
                    submit_del = st.form_submit_button(t("btn_delete_admin"), key="admin_delete_submit_v2")

                if submit_del:
                    if not selected:
                        st.warning(t("warn_select_first"))
                    elif not confirm:
                        st.warning(t("warn_confirm_first"))
                    else:
                        try:
                            selected_rowids = [int(s.split("|",1)[0].strip()) for s in selected]
                        except Exception as e:
                            st.error(t("parse_rowid_fail", e=e))
                            selected_rowids = []

                        if not selected_rowids:
                            st.warning(t("no_valid_rowid"))
                        else:
                            placeholders = ",".join(str(int(r)) for r in selected_rowids)
                            st.write(t("delete_req_rowids"), selected_rowids)
                            select_verify_sql = f"SELECT rowid, * FROM quotations WHERE rowid IN ({placeholders})"
                            try:
                                matched_df = pd.read_sql(select_verify_sql, engine)
                                if matched_df.empty:
                                    st.warning(t("no_match_cancel"))
                                    st.write("SELECT SQL:", select_verify_sql)
                                else:
                                    st.markdown(t("matched_to_delete"))
                                    safe_st_dataframe(matched_df)
                            except Exception as e:
                                st.error(f"SELECT error: {e}")
                                matched_df = pd.DataFrame()

                            if matched_df.empty:
                                st.info(t("no_deletable_stop"))
                            else:
                                try:
                                    with engine.begin() as conn:
                                        conn.execute(text("""
                                            CREATE TABLE IF NOT EXISTS deleted_quotations (
                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                original_rowid INTEGER,
                                                序号 TEXT,
                                                设备材料名称 TEXT,
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
                                                地区 TEXT,
                                                deleted_at TEXT,
                                                deleted_by TEXT
                                            )
                                        """))
                                        conn.execute(text(f"""
                                            INSERT INTO deleted_quotations (
                                                original_rowid, 序号, 设备材料名称, 规格或型号, 描述, 品牌, 单位, 数量确认,
                                                报价品牌, 型号, 设备单价, 设备小计, 人工包干单价, 人工包干小计, 综合单价汇总,
                                                币种, 原厂品牌维保期限, 货期, 备注, 询价人, 项目名称, 供应商名称, 询价日期, 录入人, 地区,
                                                deleted_at, deleted_by
                                            )
                                            SELECT
                                                rowid, 序号, 设备材料名称, 规格或型号, 描述, 品牌, 单位, 数量确认,
                                                报价品牌, 型号, 设备单价, 设备小计, 人工包干单价, 人工包干小计, 综合单价汇总,
                                                币种, 原厂品牌维保期限, 货期, 备注, 询价人, 项目名称, 供应商名称, 询价日期, 录入人, 地区,
                                                CURRENT_TIMESTAMP, :user
                                            FROM quotations WHERE rowid IN ({placeholders})
                                        """), {"user": user["username"]})
                                    st.write(t("archive_done"))
                                except Exception as e_arch:
                                    st.warning(t("archive_fail", e=e_arch))

                                delete_sql = f"DELETE FROM quotations WHERE rowid IN ({placeholders})"
                                try:
                                    with engine.begin() as conn:
                                        res = conn.execute(text(delete_sql))
                                        deleted_count = getattr(res, "rowcount", None)
                                    st.write(t("delete_sql_done"), delete_sql)
                                    st.write(t("delete_rowcount"), deleted_count)
                                except Exception as e_del:
                                    st.error(t("delete_exec_fail", e=e_del))
                                    deleted_count = None

                                try:
                                    after_df = pd.read_sql(select_verify_sql, engine)
                                    if after_df.empty:
                                        st.success(t("after_check_ok"))
                                    else:
                                        st.warning(t("after_check_warn"))
                                        safe_st_dataframe(after_df)
                                        st.write("Still existing rowids:", after_df["rowid"].tolist())
                                except Exception as e_after:
                                    st.warning(t("after_check_fail", e=e_after))

                                safe_rerun()
            else:
                st.info(t("not_admin_delete"))


# ============ Misc costs page ============
elif page == "misc":
    st.header(t("misc_search"))
    pj2 = st.text_input(t("filter_project"), key="misc_pj")
    if st.button(t("btn_search_misc"), key="misc_search"):
        params = {"pj": f"%{pj2.lower()}%"}
        sql = "SELECT id, 项目名称, 杂费类目, 金额, 币种, 录入人, 地区, 发生日期 FROM misc_costs WHERE LOWER(项目名称) LIKE :pj"
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
            st.download_button(t("btn_download_misc"), buf2, "misc_costs.xlsx", key="download_misc")


# ============ Admin page ============
elif page == "admin" and user["role"] == "admin":
    st.header(t("admin_console"))

    users_df = pd.read_sql("SELECT id, username, role, region FROM users ORDER BY id", engine)
    safe_st_dataframe(users_df)

    st.markdown("---")
    st.subheader(t("update_region_title"))

    region_options = ["Singapore","Malaysia","Thailand","Indonesia","Vietnam","Philippines","Others","All"]

    user_choices = [
        f"{row['id']} | {row['username']} | {row['role']} | {row['region']}"
        for _, row in users_df.iterrows()
    ]

    with st.form("admin_update_user_region_form"):
        target = st.selectbox(t("select_user"), user_choices, key="admin_update_user_select")
        new_region = st.selectbox(t("new_region"), region_options, key="admin_update_user_region")
        confirm_update = st.checkbox(t("confirm_update"), key="admin_update_user_confirm")
        submit_update = st.form_submit_button(t("btn_update_region"))

    if submit_update:
        try:
            target_id = int(target.split("|", 1)[0].strip())
            target_row = users_df[users_df["id"] == target_id]
            if target_row.empty:
                st.error(t("user_not_found"))
            else:
                target_username = str(target_row.iloc[0]["username"])
                target_role = str(target_row.iloc[0]["role"])

                if target_role == "admin" and target_username == "admin":
                    st.warning(t("protect_admin"))
                elif not confirm_update:
                    st.warning(t("need_confirm_update"))
                else:
                    with engine.begin() as conn:
                        conn.execute(
                            text("UPDATE users SET region=:r WHERE id=:id"),
                            {"r": new_region, "id": target_id}
                        )
                    st.success(t("update_ok", u=target_username, r=new_region))
                    st.rerun()
        except Exception as e:
            st.error(t("update_fail", e=e))

    st.markdown("---")
    st.subheader(t("delete_user_title"))
    st.caption(t("delete_user_caption"))

    protected_usernames = {user["username"], "admin"}
    deletable_rows = users_df[~users_df["username"].isin(protected_usernames)].copy()

    if deletable_rows.empty:
        st.info(t("no_deletable_users"))
    else:
        del_choices = [
            f"{row['id']} | {row['username']} | {row['role']} | {row['region']}"
            for _, row in deletable_rows.iterrows()
        ]

        with st.form("admin_delete_users_form"):
            selected = st.multiselect(t("select_users_to_delete"), del_choices, key="admin_delete_users_select")
            confirm_del = st.checkbox(t("confirm_delete_users"), key="admin_delete_users_confirm")
            submit_del = st.form_submit_button(t("btn_delete_users"))

        if submit_del:
            if not selected:
                st.warning("Please select users first." if st.session_state.lang == "en" else "请先选择要删除的用户。")
            elif not confirm_del:
                st.warning("Please confirm before deleting." if st.session_state.lang == "en" else "请勾选确认框后再删除。")
            else:
                try:
                    del_ids = [int(s.split("|", 1)[0].strip()) for s in selected]
                    check_df = users_df[users_df["id"].isin(del_ids)]
                    bad = check_df[check_df["username"].isin(protected_usernames)]
                    if not bad.empty:
                        st.error(t("reject_protected"))
                    else:
                        placeholders = ",".join(str(i) for i in del_ids)
                        with engine.begin() as conn:
                            conn.execute(text(f"DELETE FROM users WHERE id IN ({placeholders})"))
                        st.success(t("delete_users_ok", n=len(del_ids)))
                        st.rerun()
                except Exception as e:
                    st.error(t("delete_users_fail", e=e))
