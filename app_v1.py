# -*- coding: utf-8 -*-
"""
Streamlit App (Postgres / Neon version) — bilingual (CN/EN switchable)
- Keeps ALL functional modules unchanged (login/upload/import/search/misc/admin).
- Keeps database schema unchanged (Chinese column names stay the same).
- User accounts are created by admin only.
- Adds website language switcher for users (中文 / English).
- Adds bilingual keyword/alias search (e.g. 海康 / hikvision / hkvision).
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
st.set_page_config(page_title="CMI Quotation Input & Query Platform", layout="wide")


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


# ==================== I18N ====================
LANG_OPTIONS = {
    "中文": "zh",
    "English": "en"
}

I18N = {
    "zh": {
        "app_title": "CMI 询价录入与查询平台",
        "app_subtitle": "High Tech • Effective Solution • Quick Use",
        "lang_label": "语言",
        "help_title": "使用说明",
        "help_text": "• 普通用户账号由管理员统一创建与分配<br/>• 登录后可批量导入询价记录 / 查询 / 下载结果<br/>• 管理员可创建账号、删除记录、管理用户",
        "login_tab": "🔑 登录",
        "login_system": "登录系统",
        "login_sub": "使用你的账号进入询价录入与查询平台",
        "username": "用户名",
        "password": "密码",
        "login_btn": "登录",
        "enter_user_pass": "请输入用户名和密码",
        "wrong_user_pass": "用户名或密码错误",
        "current_user": "当前用户",
        "logout": "退出登录",
        "page_input": "🏠 录入页面",
        "page_device_query": "📋 设备查询",
        "page_misc_query": "💰 杂费查询",
        "page_admin": "👑 管理员后台",
        "input_center": "录入中心",
        "input_center_sub": "支持 Excel 批量录入 + 手工录入（设备 / 杂费）",
        "excel_bulk": "📂 Excel 批量录入",
        "excel_caption": "系统会尝试识别上传文件的表头并给出建议映射。",
        "download_template": "下载模板",
        "upload_excel": "上传 Excel (.xlsx)",
        "preview_fail": "读取预览失败：{}",
        "raw_headers_detected": "检测到的原始表头（用于映射，系统已尝试自动对应一版建议）：",
        "mapping_suggested": "系统已为每一列生成建议映射（你可以直接应用，或修改后提交）。",
        "source_col": "源列: {}",
        "ignore": "Ignore",
        "apply_mapping_preview": "应用映射并预览",
        "mapping_saved": "映射已保存。现在请填写全局必填信息并导入。",
        "mapping_restore_fail": "恢复映射数据失败：{}",
        "mapped_preview": "映射后预览（前 10 行）：",
        "mapping_unavailable": "映射数据无法预览，请重新映射。",
        "open_global_form": "➡️ 填写/查看全局信息并应用导入",
        "global_hint": "若需要对空值统一填充（币种/项目/供应商/询价人），请展开并填写全局信息。",
        "global_info": "#### 全局信息（仅填充空值）",
        "project_name": "项目名称",
        "supplier_name": "供应商名称",
        "enquirer": "询价人",
        "inq_date": "询价日期",
        "currency_fill": "币种（用于填充空值）",
        "apply_global_check": "应用全局并继续校验",
        "global_required": "必须填写：项目名称、供应商名称、询价人和询价日期",
        "global_currency_required": "由于源数据存在空的币种，请选择币种以继续。",
        "global_applied": "已应用全局信息，正在进行总体必填校验...",
        "mapping_lost": "映射数据丢失，无法继续导入。",
        "imported_valid": "✅ 已导入 {} 条有效记录。",
        "import_valid_fail": "导入有效记录时发生错误：{}",
        "no_valid_records": "没有找到满足总体必填条件的记录可导入。",
        "invalid_rows_warn": "以下 {} 条记录缺少总体必填字段，未被导入，请修正后重新导入：",
        "download_invalid": "📥 下载未通过记录（用于修正）",
        "manual_device": "✏️ 手工录入设备",
        "material_name": "设备材料名称",
        "brand_optional": "品牌（可选）",
        "qty_confirm": "数量确认",
        "device_unit_price": "设备单价",
        "labor_unit_price": "人工包干单价",
        "currency": "币种",
        "description": "描述",
        "manual_add_btn": "添加记录（手动）",
        "manual_required": "必填项不能为空：项目名称、供应商名称、询价人、设备材料名称",
        "manual_price_required": "请至少填写 设备单价 或 人工包干单价（两者至少填一项，且大于0）。",
        "manual_add_success": "✅ 手工记录已添加",
        "manual_add_fail": "添加记录失败：{}",
        "manual_misc": "💰 手工录入杂费",
        "misc_category": "杂费类目（例如运输/安装/税费）",
        "amount": "金额",
        "remark_optional": "备注（可选）",
        "occ_date": "发生日期",
        "add_misc_btn": "添加杂费记录",
        "misc_required": "请填写项目名称、杂费类目和金额",
        "misc_add_success": "✅ 杂费记录已添加",
        "misc_add_fail": "添加杂费记录失败：{}",
        "device_query_title": "设备查询",
        "device_query_sub": "按关键词 / 项目 / 供应商 / 品牌 / 地区筛选，并支持导出 Excel",
        "keyword_multi": "关键词（多个空格分词）",
        "search_fields": "搜索字段（留空为默认）",
        "filter_project": "按项目名称过滤",
        "filter_supplier": "按供应商名称过滤",
        "filter_brand": "按品牌过滤",
        "all": "全部",
        "filter_region_admin": "按地区过滤（管理员）",
        "only_region_data": "仅显示您所在地区的数据：{}",
        "search_device_btn": "🔍 搜索设备",
        "query_fail": "查询失败：{}",
        "no_records": "未找到符合条件的记录。",
        "download_result": "下载结果",
        "price_stats": "### 当前查询 — 价格统计概览（基于返回记录）",
        "device_avg": "设备单价 — 均价",
        "device_min": "设备单价 — 最低价",
        "labor_avg": "人工包干单价 — 均价",
        "labor_min": "人工包干单价 — 最低价",
        "device_min_rows": "#### 设备单价 — 历史最低价对应记录（可能多条并列）",
        "labor_min_rows": "#### 人工包干单价 — 历史最低价对应记录（可能多条并列）",
        "group_by_name": "#### 按设备名称分组 — 均价 / 最低价",
        "stats_fail": "计算价格统计时发生异常：{}",
        "search_alias_caption": "支持中英文关键词、品牌别名和常见缩写搜索，例如：海康 / hikvision / hkvision",
        "admin_delete_title": "### ⚠️ 管理员删除（按 id 删除）",
        "admin_select_delete": "选中要删除的记录",
        "admin_confirm_delete": "我确认删除所选记录（不可恢复）",
        "admin_delete_btn": "删除所选记录（管理员）",
        "select_delete_first": "请先选择要删除的记录。",
        "confirm_delete_first": "请勾选确认框以执行删除。",
        "parse_id_fail": "解析所选 id 失败：{}",
        "invalid_id_cancel": "无有效 id，取消删除。",
        "delete_archive_success": "✅ 已删除并归档所选记录。",
        "delete_archive_fail": "删除/归档失败：{}",
        "only_admin_delete": "仅管理员可删除记录。",
        "misc_query_title": "杂费查询",
        "misc_query_sub": "按项目名称检索杂费记录，支持导出 Excel",
        "search_misc_btn": "🔍 搜索杂费",
        "download_misc": "下载杂费结果",
        "admin_panel": "管理员后台",
        "admin_panel_sub": "用户管理：创建 / 查看 / 修改地区 / 删除账号（保护当前用户与默认 admin）",
        "admin_user_mgmt": "👑 管理员后台 — 用户管理",
        "update_region_title": "🛠️ 修改用户地区（Region）",
        "select_user_update": "选择要修改的用户",
        "new_region": "新地区",
        "confirm_update_region": "我确认要修改该用户地区",
        "update_region_btn": "更新地区",
        "user_not_found": "未找到该用户，请刷新页面。",
        "default_admin_warn": "系统默认 admin 不建议修改地区。",
        "confirm_update_first": "请勾选确认框后再更新。",
        "update_region_success": "✅ 已更新用户 {} 的地区为：{}",
        "update_fail": "更新失败：{}",
        "delete_user_title": "🗑️ 删除用户账号",
        "delete_user_caption": "说明：删除账号不会自动删除该用户已录入的报价/杂费数据（数据仍保留在 quotations / misc_costs 表中）。",
        "no_deletable_user": "当前没有可删除的用户（已保护当前登录用户与默认 admin）。",
        "select_delete_user": "选择要删除的用户（可多选）",
        "confirm_delete_user": "我确认删除所选用户（不可恢复）",
        "delete_user_btn": "删除用户",
        "protected_user_error": "所选用户包含受保护账号（当前登录用户或默认 admin），已拒绝删除。",
        "delete_user_success": "✅ 已删除 {} 个用户账号",
        "db_missing": "缺少数据库连接：请在 Streamlit Secrets 或环境变量中设置 DB_URL。",
        "user": "用户",
        "role": "角色",
        "admin_create_user_title": "➕ 新增用户账号",
        "admin_create_user_sub": "普通用户账号由管理员统一创建并分配地区",
        "admin_create_username": "用户名",
        "admin_create_password": "密码",
        "admin_create_region": "地区",
        "admin_create_confirm": "我确认新增该用户账号",
        "admin_create_btn": "创建用户",
        "admin_create_success": "✅ 已创建用户 {}，地区为：{}",
        "admin_create_fail": "创建用户失败：用户名已存在或数据库异常",
        "admin_create_confirm_first": "请勾选确认框后再创建用户",
        "empty_user_pass": "用户名和密码不能为空",
    },
    "en": {
        "app_title": "CMI Quotation Input & Query Platform",
        "app_subtitle": "High Tech • Effective Solution • Quick Use",
        "lang_label": "Language",
        "help_title": "Instructions",
        "help_text": "• Normal user accounts are created and assigned by admin only<br/>• After login, you can batch import quotation records / search / download results<br/>• Admins can create accounts, delete records, and manage users",
        "login_tab": "🔑 Login",
        "login_system": "Login",
        "login_sub": "Use your account to access the quotation input and query platform",
        "username": "Username",
        "password": "Password",
        "login_btn": "Login",
        "enter_user_pass": "Please enter username and password",
        "wrong_user_pass": "Incorrect username or password",
        "current_user": "Current User",
        "logout": "Logout",
        "page_input": "🏠 Input",
        "page_device_query": "📋 Device Query",
        "page_misc_query": "💰 Misc Query",
        "page_admin": "👑 Admin Panel",
        "input_center": "Input Center",
        "input_center_sub": "Supports Excel batch import + manual input (devices / miscellaneous costs)",
        "excel_bulk": "📂 Excel Batch Import",
        "excel_caption": "The system will try to detect the headers in the uploaded file and suggest mappings.",
        "download_template": "Download Template",
        "upload_excel": "Upload Excel (.xlsx)",
        "preview_fail": "Failed to read preview: {}",
        "raw_headers_detected": "Detected raw headers (for mapping; the system has already suggested a first-round mapping):",
        "mapping_suggested": "Suggested mappings have been generated for each column. You may apply them directly or adjust them before submitting.",
        "source_col": "Source Column: {}",
        "ignore": "Ignore",
        "apply_mapping_preview": "Apply Mapping and Preview",
        "mapping_saved": "Mapping has been saved. Please fill in the required global information and then import.",
        "mapping_restore_fail": "Failed to restore mapped data: {}",
        "mapped_preview": "Mapped Preview (first 10 rows):",
        "mapping_unavailable": "Mapped data cannot be previewed. Please remap it.",
        "open_global_form": "➡️ Fill / View Global Information and Import",
        "global_hint": "If you need to fill blank values in batch (currency/project/supplier/enquirer), please expand and complete the global information.",
        "global_info": "#### Global Information (fill blank values only)",
        "project_name": "Project Name",
        "supplier_name": "Supplier Name",
        "enquirer": "Enquirer",
        "inq_date": "Enquiry Date",
        "currency_fill": "Currency (used to fill blank values)",
        "apply_global_check": "Apply Global Info and Continue Validation",
        "global_required": "Required: Project Name, Supplier Name, Enquirer, and Enquiry Date",
        "global_currency_required": "Some source rows have empty currency values. Please select a currency to continue.",
        "global_applied": "Global information applied. Running overall required-field validation...",
        "mapping_lost": "Mapped data is missing and cannot continue import.",
        "imported_valid": "✅ Imported {} valid records.",
        "import_valid_fail": "Error while importing valid records: {}",
        "no_valid_records": "No records meeting the overall required conditions were found for import.",
        "invalid_rows_warn": "The following {} rows are missing required fields and were not imported. Please correct them and re-import:",
        "download_invalid": "📥 Download Invalid Rows (for correction)",
        "manual_device": "✏️ Manual Device Input",
        "material_name": "Device / Material Name",
        "brand_optional": "Brand (optional)",
        "qty_confirm": "Confirmed Quantity",
        "device_unit_price": "Device Unit Price",
        "labor_unit_price": "Labor Lump-Sum Unit Price",
        "currency": "Currency",
        "description": "Description",
        "manual_add_btn": "Add Record (Manual)",
        "manual_required": "Required fields cannot be empty: Project Name, Supplier Name, Enquirer, Device / Material Name",
        "manual_price_required": "Please enter at least one of Device Unit Price or Labor Lump-Sum Unit Price, and it must be greater than 0.",
        "manual_add_success": "✅ Manual record added",
        "manual_add_fail": "Failed to add record: {}",
        "manual_misc": "💰 Manual Miscellaneous Cost Input",
        "misc_category": "Miscellaneous Cost Category (e.g. transport / installation / tax)",
        "amount": "Amount",
        "remark_optional": "Remark (optional)",
        "occ_date": "Occurrence Date",
        "add_misc_btn": "Add Misc Cost Record",
        "misc_required": "Please fill in Project Name, Miscellaneous Cost Category, and Amount",
        "misc_add_success": "✅ Miscellaneous cost record added",
        "misc_add_fail": "Failed to add miscellaneous cost record: {}",
        "device_query_title": "Device Query",
        "device_query_sub": "Filter by keyword / project / supplier / brand / region, with Excel export support",
        "keyword_multi": "Keyword(s) (split by spaces)",
        "search_fields": "Search Fields (leave blank for default)",
        "filter_project": "Filter by Project Name",
        "filter_supplier": "Filter by Supplier Name",
        "filter_brand": "Filter by Brand",
        "all": "All",
        "filter_region_admin": "Filter by Region (Admin)",
        "only_region_data": "Only data from your region is shown: {}",
        "search_device_btn": "🔍 Search Devices",
        "query_fail": "Query failed: {}",
        "no_records": "No matching records found.",
        "download_result": "Download Results",
        "price_stats": "### Current Query — Price Statistics Overview (based on returned records)",
        "device_avg": "Device Unit Price — Average",
        "device_min": "Device Unit Price — Minimum",
        "labor_avg": "Labor Lump-Sum Unit Price — Average",
        "labor_min": "Labor Lump-Sum Unit Price — Minimum",
        "device_min_rows": "#### Device Unit Price — Records corresponding to the historical minimum price (ties possible)",
        "labor_min_rows": "#### Labor Lump-Sum Unit Price — Records corresponding to the historical minimum price (ties possible)",
        "group_by_name": "#### Grouped by Device Name — Average / Minimum",
        "stats_fail": "Exception occurred while calculating price statistics: {}",
        "search_alias_caption": "Supports Chinese/English keywords, brand aliases, and common abbreviations, e.g. 海康 / hikvision / hkvision",
        "admin_delete_title": "### ⚠️ Admin Delete (delete by id)",
        "admin_select_delete": "Select records to delete",
        "admin_confirm_delete": "I confirm deletion of the selected records (cannot be undone)",
        "admin_delete_btn": "Delete Selected Records (Admin)",
        "select_delete_first": "Please select records to delete first.",
        "confirm_delete_first": "Please tick the confirmation checkbox before deleting.",
        "parse_id_fail": "Failed to parse selected id(s): {}",
        "invalid_id_cancel": "No valid id found. Deletion cancelled.",
        "delete_archive_success": "✅ Selected records have been deleted and archived.",
        "delete_archive_fail": "Delete / archive failed: {}",
        "only_admin_delete": "Only admins can delete records.",
        "misc_query_title": "Miscellaneous Cost Query",
        "misc_query_sub": "Search miscellaneous cost records by project name, with Excel export support",
        "search_misc_btn": "🔍 Search Misc Costs",
        "download_misc": "Download Misc Results",
        "admin_panel": "Admin Panel",
        "admin_panel_sub": "User management: create / view / update region / delete accounts (protect current user and default admin)",
        "admin_user_mgmt": "👑 Admin Panel — User Management",
        "update_region_title": "🛠️ Update User Region",
        "select_user_update": "Select user to update",
        "new_region": "New Region",
        "confirm_update_region": "I confirm updating this user's region",
        "update_region_btn": "Update Region",
        "user_not_found": "User not found. Please refresh the page.",
        "default_admin_warn": "It is not recommended to modify the region of the default admin.",
        "confirm_update_first": "Please tick the confirmation checkbox before updating.",
        "update_region_success": "✅ Updated user {} region to: {}",
        "update_fail": "Update failed: {}",
        "delete_user_title": "🗑️ Delete User Account",
        "delete_user_caption": "Note: deleting an account will not automatically delete the quotation / miscellaneous cost data entered by that user. The data remains in the quotations / misc_costs tables.",
        "no_deletable_user": "There are currently no deletable users (current logged-in user and default admin are protected).",
        "select_delete_user": "Select user(s) to delete (multiple selection allowed)",
        "confirm_delete_user": "I confirm deleting the selected user(s) (cannot be undone)",
        "delete_user_btn": "Delete User(s)",
        "protected_user_error": "The selected users contain protected accounts (current logged-in user or default admin). Deletion rejected.",
        "delete_user_success": "✅ Deleted {} user account(s)",
        "db_missing": "Database connection is missing. Please set DB_URL in Streamlit Secrets or environment variables.",
        "user": "User",
        "role": "Role",
        "admin_create_user_title": "➕ Create User Account",
        "admin_create_user_sub": "Normal user accounts are created by admin and assigned to a region",
        "admin_create_username": "Username",
        "admin_create_password": "Password",
        "admin_create_region": "Region",
        "admin_create_confirm": "I confirm creating this user account",
        "admin_create_btn": "Create User",
        "admin_create_success": "✅ User {} has been created with region: {}",
        "admin_create_fail": "Failed to create user: username already exists or database error",
        "admin_create_confirm_first": "Please tick the confirmation checkbox before creating the user",
        "empty_user_pass": "Username and password cannot be empty",
    }
}


if "lang" not in st.session_state:
    st.session_state["lang"] = "en"


def t(key: str) -> str:
    lang = st.session_state.get("lang", "en")
    return I18N.get(lang, I18N["en"]).get(key, key)


def set_language_widget(key_name: str):
    reverse_map = {v: k for k, v in LANG_OPTIONS.items()}
    current_display = reverse_map.get(st.session_state.get("lang", "en"), "English")
    selected = st.selectbox(
        t("lang_label"),
        options=list(LANG_OPTIONS.keys()),
        index=list(LANG_OPTIONS.keys()).index(current_display),
        key=key_name
    )
    st.session_state["lang"] = LANG_OPTIONS[selected]


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
    st.error(t("db_missing"))
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
    """), {"pw": hashlib.sha256("admin".encode()).hexdigest()})


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

# ==================== SEARCH SYNONYMS ====================
SYNONYM_GROUPS = [
    ["海康威视", "海康", "hikvision", "hkvision"],
    ["华为", "huawei"],
    ["中兴", "zte", "zte corporation"],
    ["新华三", "h3c"],
    ["思科", "cisco"],
    ["瞻博", "juniper"],
    ["锐捷", "ruijie"],
    ["戴尔", "dell", "dell emc"],
    ["惠普", "hp", "hewlett packard", "hpe"],
    ["联想", "lenovo"],
    ["浪潮", "inspur"],
    ["深信服", "sangfor"],
    ["飞塔", "fortinet"],
    ["帕洛阿尔托", "palo alto", "palo alto networks", "pan"],
    ["阿里云", "aliyun", "alibaba cloud"],
    ["腾讯云", "tencent cloud"],
    ["亚马逊云", "aws", "amazon web services"],
    ["微软云", "azure", "microsoft azure"],
    ["谷歌云", "gcp", "google cloud"],
]

SYNONYM_MAP = {}
for group in SYNONYM_GROUPS:
    normalized_group = []
    for item in group:
        s = str(item).strip().lower()
        if s:
            normalized_group.append(s)
    normalized_group = list(dict.fromkeys(normalized_group))
    for item in normalized_group:
        SYNONYM_MAP[item] = normalized_group


def expand_keywords(token: str):
    if token is None:
        return []
    t0 = str(token).strip()
    t = t0.lower()
    if not t:
        return []
    if t in SYNONYM_MAP:
        return SYNONYM_MAP[t]
    return [t]


def expand_query_tokens(tokens):
    expanded = []
    seen = set()
    for token in tokens:
        for item in expand_keywords(token):
            if item not in seen:
                seen.add(item)
                expanded.append(item)
    return expanded


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
REGION_OPTIONS = ["Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Philippines", "Others"]
REGION_OPTIONS_ADMIN = ["Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam", "Philippines", "Others", "All"]
CURRENCY_OPTIONS = ["IDR", "USD", "RMB", "SGD", "MYR", "THB"]


def login_form():
    ui_card(t("login_system"), t("login_sub"))
    ui_hr()

    with st.form("login_form"):
        u = st.text_input(t("username"))
        p = st.text_input(t("password"), type="password")
        submitted = st.form_submit_button(t("login_btn"))

    if submitted:
        if not u or not p:
            st.error(t("enter_user_pass"))
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
            st.error(t("wrong_user_pass"))


def logout():
    if "user" in st.session_state:
        del st.session_state["user"]
    safe_rerun()


# ==================== PAGE FLOW ====================
if "user" not in st.session_state:
    top_lang_l, top_lang_r = st.columns([6, 1.4])
    with top_lang_r:
        set_language_widget("lang_selector_guest_top")

    left, right = st.columns([1.35, 1])
    with left:
        st.markdown(f"## ✨ {t('app_title')}")
    with right:
        st.markdown(
            f"""
            <div class="card">
              <div class="title">{t("help_title")}</div>
              <div class="sub">
                {t("help_text")}
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    ui_hr()
    login_form()
    st.stop()

user = st.session_state["user"]

# ==================== TOP BAR ====================
top_l, top_m, top_blank = st.columns([1.4, 2.3, 1.1])

with top_blank:
    set_language_widget("lang_selector_user_top")

with top_m:
    pages = [t("page_input"), t("page_device_query"), t("page_misc_query")]
    if user["role"] == "admin":
        pages.append(t("page_admin"))
    nav_tabs = st.tabs(pages)

with top_l:
    st.markdown(
        f"""
        <div class="card">
          <div class="title">{t("app_title")}</div>
          <div class="sub">{t("app_subtitle")}</div>
          <div class="hr" style="margin:0.75rem 0 0.8rem 0;"></div>
          <div class="title" style="font-size:0.98rem;">{t("current_user")}</div>
          <div class="sub">
            👤 {user["username"]}<br/>
            🏢 {user["region"]}<br/>
            🔑 {user["role"]}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    if st.button(t("logout"), key="logout_btn_top"):
        logout()

ui_hr()


# ==================== PAGE: HOME / INPUT ====================
with nav_tabs[0]:
    ui_card(t("input_center"), t("input_center_sub"))
    ui_hr()

    st.header(t("excel_bulk"))
    st.caption(t("excel_caption"))

    template = pd.DataFrame(columns=[c for c in DB_COLUMNS if c not in ("录入人", "地区")])
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        template.to_excel(writer, index=False)
    buf.seek(0)
    st.download_button(t("download_template"), buf, "quotation_template.xlsx", key="download_template")

    uploaded = st.file_uploader(t("upload_excel"), type=["xlsx"], key="upload_excel")

    if uploaded:
        if "mapping_done" not in st.session_state:
            st.session_state["mapping_done"] = False
        if "bulk_applied" not in st.session_state:
            st.session_state["bulk_applied"] = False

        try:
            preview = pd.read_excel(uploaded, header=None, nrows=50, dtype=object)
            safe_st_dataframe(preview.head(10), height=320)
        except Exception as e:
            st.error(t("preview_fail").format(e))
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

            st.markdown(f"**{t('raw_headers_detected')}**")
            st.write(list(data_df.columns))

            mapping_targets = [t("ignore")] + [c for c in DB_COLUMNS if c not in ("录入人", "地区")]

            auto_defaults = {}
            for col in data_df.columns:
                auto_val = auto_map_header(col)
                auto_defaults[col] = auto_val if (auto_val and auto_val in mapping_targets) else t("ignore")

            st.markdown(t("mapping_suggested"))

            mapped_choices = {}
            with st.form("mapping_form", clear_on_submit=False):
                cols_left, cols_right = st.columns(2)
                for i, col in enumerate(data_df.columns):
                    default = auto_defaults.get(col, t("ignore"))
                    container = cols_left if i % 2 == 0 else cols_right
                    sel = container.selectbox(
                        t("source_col").format(col),
                        mapping_targets,
                        index=mapping_targets.index(default) if default in mapping_targets else 0,
                        key=f"map_{i}"
                    )
                    mapped_choices[col] = sel
                submitted = st.form_submit_button(t("apply_mapping_preview"))

            if submitted:
                rename_dict = {orig: mapped for orig, mapped in mapped_choices.items() if mapped != t("ignore")}
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

                st.success(t("mapping_saved"))

    mapping_csv = st.session_state.get("mapping_csv", None)
    if mapping_csv:
        try:
            df_for_db = pd.read_csv(io.StringIO(mapping_csv), dtype=object)
            for c in DB_COLUMNS:
                if c not in df_for_db.columns:
                    df_for_db[c] = pd.NA
            df_for_db = df_for_db[DB_COLUMNS]
        except Exception as e:
            st.error(t("mapping_restore_fail").format(e))
            df_for_db = None

        st.markdown(f"**{t('mapped_preview')}**")
        if df_for_db is not None:
            safe_st_dataframe(df_for_db.head(10), height=320)
        else:
            st.info(t("mapping_unavailable"))

        if "show_global_form" not in st.session_state:
            st.session_state["show_global_form"] = False

        col_show, col_hint = st.columns([1, 6])
        if col_show.button(t("open_global_form"), key="open_global_form_btn"):
            st.session_state["show_global_form"] = True
        col_hint.caption(t("global_hint"))

        if st.session_state["show_global_form"]:
            if "bulk_values" not in st.session_state:
                st.session_state["bulk_values"] = {"project": "", "supplier": "", "enquirer": "", "date": "", "currency": ""}

            def column_has_empty_currency(df: pd.DataFrame) -> bool:
                if df is None or "币种" not in df.columns:
                    return True
                ser = df["币种"]
                return ser.map(lambda x: normalize_cell(x) is None).any()

            need_global_currency = column_has_empty_currency(df_for_db)

            st.markdown(t("global_info"))
            with st.form("global_form_v2"):
                g1, g2, g3, g4, g5 = st.columns(5)
                g_project = g1.text_input(t("project_name"), value=st.session_state["bulk_values"].get("project", ""))
                g_supplier = g2.text_input(t("supplier_name"), value=st.session_state["bulk_values"].get("supplier", ""))
                g_enquirer = g3.text_input(t("enquirer"), value=st.session_state["bulk_values"].get("enquirer", ""))

                default_date = st.session_state["bulk_values"].get("date", "")
                try:
                    g_date = g4.date_input(t("inq_date"), value=pd.to_datetime(default_date).date() if default_date else date.today())
                except Exception:
                    g_date = g4.date_input(t("inq_date"), value=date.today())

                g_currency = None
                if need_global_currency:
                    currency_options = [""] + CURRENCY_OPTIONS
                    curr_default = st.session_state["bulk_values"].get("currency", "")
                    default_idx = currency_options.index(curr_default) if curr_default in currency_options else 0
                    g_currency = g5.selectbox(t("currency_fill"), currency_options, index=default_idx)
                else:
                    g5.write("")

                apply_global = st.form_submit_button(t("apply_global_check"))

            if apply_global:
                if not (g_project and g_supplier and g_enquirer and g_date):
                    st.error(t("global_required"))
                    st.session_state["bulk_applied"] = False
                elif need_global_currency and (g_currency is None or str(g_currency).strip() == ""):
                    st.error(t("global_currency_required"))
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
                    st.success(t("global_applied"))

            if st.session_state.get("bulk_applied", False):
                try:
                    df_for_db2 = pd.read_csv(io.StringIO(st.session_state["mapping_csv"]), dtype=object)
                    for c in DB_COLUMNS:
                        if c not in df_for_db2.columns:
                            df_for_db2[c] = pd.NA
                    df_for_db2 = df_for_db2[DB_COLUMNS]
                except Exception as e:
                    st.error(t("mapping_restore_fail").format(e))
                    df_for_db2 = None

                if df_for_db2 is None:
                    st.error(t("mapping_lost"))
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
                            st.success(t("imported_valid").format(imported_count))
                        except Exception as e:
                            st.error(t("import_valid_fail").format(e))
                    else:
                        st.info(t("no_valid_records"))

                    if not df_invalid.empty:
                        st.warning(t("invalid_rows_warn").format(len(df_invalid)))
                        safe_st_dataframe(df_invalid.head(50), height=360)
                        buf_bad = io.BytesIO()
                        with pd.ExcelWriter(buf_bad, engine="openpyxl") as w:
                            df_invalid.to_excel(w, index=False)
                        buf_bad.seek(0)
                        st.download_button(t("download_invalid"), buf_bad, "invalid_rows.xlsx")

                    st.session_state["bulk_applied"] = False

    ui_hr()
    st.header(t("manual_device"))
    with st.form("manual_add_form_original", clear_on_submit=True):
        col1, col2, col3 = st.columns(3)
        pj = col1.text_input(t("project_name"))
        sup = col2.text_input(t("supplier_name"))
        inq = col3.text_input(t("enquirer"))
        name = st.text_input(t("material_name"))
        brand = st.text_input(t("brand_optional"))
        qty = st.number_input(t("qty_confirm"), min_value=0.0)
        price = st.number_input(t("device_unit_price"), min_value=0.0)
        labor_price = st.number_input(t("labor_unit_price"), min_value=0.0)
        cur = st.selectbox(t("currency"), CURRENCY_OPTIONS)
        desc = st.text_area(t("description"))
        date_inq = st.date_input(t("inq_date"), value=date.today())
        submit_manual = st.form_submit_button(t("manual_add_btn"))

    if submit_manual:
        if not (pj and sup and inq and name):
            st.error(t("manual_required"))
        else:
            if not (price > 0 or labor_price > 0):
                st.error(t("manual_price_required"))
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
                    st.success(t("manual_add_success"))
                except Exception as e:
                    st.error(t("manual_add_fail").format(e))

    ui_hr()
    st.header(t("manual_misc"))
    with st.form("manual_misc_form", clear_on_submit=True):
        mcol1, mcol2, mcol3 = st.columns(3)
        misc_project = mcol1.text_input(t("project_name"))
        misc_category = mcol2.text_input(t("misc_category"))
        misc_amount = mcol3.number_input(t("amount"), min_value=0.0, format="%f")
        mc1, mc2 = st.columns(2)
        misc_currency = mc1.selectbox(t("currency"), CURRENCY_OPTIONS)
        misc_note = mc2.text_input(t("remark_optional"))
        misc_date = st.date_input(t("occ_date"), value=date.today())
        submit_misc = st.form_submit_button(t("add_misc_btn"))

    if submit_misc:
        if not (misc_project and misc_category) or misc_amount is None:
            st.error(t("misc_required"))
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
                st.success(t("misc_add_success"))
            except Exception as e:
                st.error(t("misc_add_fail").format(e))


# ==================== PAGE: SEARCH QUOTATIONS ====================
with nav_tabs[1]:
    ui_card(t("device_query_title"), t("device_query_sub"))
    ui_hr()

    st.header("📋 " + t("device_query_title"))

    kw = st.text_input(t("keyword_multi"), key="search_kw")
    st.caption(t("search_alias_caption"))

    search_fields = st.multiselect(
        t("search_fields"),
        ["设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称", "地区"],
        key="search_fields"
    )
    pj_filter = st.text_input(t("filter_project"), key="search_pj")
    sup_filter = st.text_input(t("filter_supplier"), key="search_sup")
    brand_filter = st.text_input(t("filter_brand"), key="search_brand")
    cur_filter = st.selectbox(t("currency"), [t("all")] + CURRENCY_OPTIONS, index=0, key="search_cur")

    regions_options = [t("all")] + REGION_OPTIONS_ADMIN
    if user["role"] == "admin":
        region_filter = st.selectbox(t("filter_region_admin"), regions_options, index=0, key="search_region")
    else:
        st.info(t("only_region_data").format(user["region"]))
        region_filter = user["region"]

    if st.button(t("search_device_btn"), key="search_button"):
        conds = []
        params = {}

        if pj_filter:
            pj_tokens = re.findall(r"\S+", pj_filter)
            pj_expanded = expand_query_tokens(pj_tokens)
            pj_ors = []
            for i, token in enumerate(pj_expanded):
                pname = f"pj_{i}"
                pj_ors.append(f"LOWER(项目名称) LIKE :{pname}")
                params[pname] = f"%{token.lower()}%"
            if pj_ors:
                conds.append("(" + " OR ".join(pj_ors) + ")")

        if sup_filter:
            sup_tokens = re.findall(r"\S+", sup_filter)
            sup_expanded = expand_query_tokens(sup_tokens)
            sup_ors = []
            for i, token in enumerate(sup_expanded):
                pname = f"sup_{i}"
                sup_ors.append(f"LOWER(供应商名称) LIKE :{pname}")
                params[pname] = f"%{token.lower()}%"
            if sup_ors:
                conds.append("(" + " OR ".join(sup_ors) + ")")

        if brand_filter:
            brand_tokens = re.findall(r"\S+", brand_filter)
            brand_expanded = expand_query_tokens(brand_tokens)
            brand_ors = []
            for i, token in enumerate(brand_expanded):
                pname = f"brand_{i}"
                brand_ors.append(f"LOWER(品牌) LIKE :{pname}")
                params[pname] = f"%{token.lower()}%"
            if brand_ors:
                conds.append("(" + " OR ".join(brand_ors) + ")")

        if cur_filter != t("all"):
            conds.append("币种 = :cur")
            params["cur"] = cur_filter

        if user["role"] != "admin":
            conds.append("地区 = :r")
            params["r"] = user["region"]
        else:
            if region_filter and region_filter != t("all"):
                conds.append("地区 = :r")
                params["r"] = region_filter

        if kw:
            raw_tokens = re.findall(r"\S+", kw)
            fields = search_fields if search_fields else [
                "设备材料名称", "描述", "品牌", "规格或型号", "项目名称", "供应商名称"
            ]

            for raw_idx, raw_token in enumerate(raw_tokens):
                token_synonyms = expand_keywords(raw_token)
                token_ors = []

                for syn_idx, synonym in enumerate(token_synonyms):
                    for field_idx, field_name in enumerate(fields):
                        pname = f"kw_{raw_idx}_{syn_idx}_{field_idx}"
                        token_ors.append(f"LOWER({field_name}) LIKE :{pname}")
                        params[pname] = f"%{synonym.lower()}%"

                if token_ors:
                    conds.append("(" + " OR ".join(token_ors) + ")")

        sql = "SELECT * FROM quotations"
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY id DESC"

        try:
            df = pd.read_sql(text(sql), engine, params=params)
        except Exception as e:
            st.error(t("query_fail").format(e))
            df = pd.DataFrame()

        if df.empty:
            st.info(t("no_records"))
        else:
            safe_st_dataframe(df, height=520)

            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            buf.seek(0)
            st.download_button(t("download_result"), buf, "device_query_results.xlsx", key="download_search")

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
                st.markdown(t("price_stats"))
                c1, c2, c3, c4 = st.columns(4)
                c1.metric(t("device_avg"), fmt(overall["dev_mean"]))
                c2.metric(t("device_min"), fmt(overall["dev_min"]))
                c3.metric(t("labor_avg"), fmt(overall["lab_mean"]))
                c4.metric(t("labor_min"), fmt(overall["lab_min"]))

                if not pd.isna(overall["dev_min"]):
                    dev_min_rows = df_prices[df_prices[device_price_col] == overall["dev_min"]].copy()
                    st.markdown(t("device_min_rows"))
                    safe_st_dataframe(dev_min_rows.reset_index(drop=True), height=260)

                if not pd.isna(overall["lab_min"]):
                    lab_min_rows = df_prices[df_prices[labor_price_col] == overall["lab_min"]].copy()
                    st.markdown(t("labor_min_rows"))
                    safe_st_dataframe(lab_min_rows.reset_index(drop=True), height=260)

                if name_col in df_prices.columns:
                    agg = df_prices.groupby(name_col).agg(
                        设备单价_均价=(device_price_col, lambda s: s.mean(skipna=True)),
                        设备单价_最低=(device_price_col, lambda s: s.min(skipna=True)),
                        人工包干单价_均价=(labor_price_col, lambda s: s.mean(skipna=True)),
                        人工包干单价_最低=(labor_price_col, lambda s: s.min(skipna=True)),
                        样本数=(device_price_col, "count")
                    ).reset_index()
                    st.markdown(t("group_by_name"))
                    safe_st_dataframe(agg.sort_values(by="设备单价_均价", ascending=True).head(200), height=360)
            except Exception as e:
                st.warning(t("stats_fail").format(e))

            if user["role"] == "admin":
                ui_hr()
                st.markdown(t("admin_delete_title"))
                choices = []
                for _, row in df.iterrows():
                    rid = int(row["id"])
                    proj = str(row.get("项目名称", ""))[:40]
                    name = str(row.get("设备材料名称", ""))[:60]
                    brand = str(row.get("品牌", ""))[:30]
                    choices.append(f"{rid} | {proj} | {name} | {brand}")

                with st.form("admin_delete_form_pg", clear_on_submit=False):
                    selected = st.multiselect(t("admin_select_delete"), choices, key="admin_delete_selected_pg")
                    confirm = st.checkbox(t("admin_confirm_delete"), key="admin_delete_confirm_pg")
                    submit_del = st.form_submit_button(t("admin_delete_btn"), key="admin_delete_submit_pg")

                if submit_del:
                    if not selected:
                        st.warning(t("select_delete_first"))
                    elif not confirm:
                        st.warning(t("confirm_delete_first"))
                    else:
                        try:
                            selected_ids = [int(s.split("|", 1)[0].strip()) for s in selected]
                        except Exception as e:
                            st.error(t("parse_id_fail").format(e))
                            selected_ids = []

                        if not selected_ids:
                            st.warning(t("invalid_id_cancel"))
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
                                st.success(t("delete_archive_success"))
                                safe_rerun()
                            except Exception as e:
                                st.error(t("delete_archive_fail").format(e))
            else:
                st.info(t("only_admin_delete"))


# ==================== PAGE: SEARCH MISC ====================
with nav_tabs[2]:
    ui_card(t("misc_query_title"), t("misc_query_sub"))
    ui_hr()

    st.header("💰 " + t("misc_query_title"))
    pj2 = st.text_input(t("filter_project"), key="misc_pj")

    if st.button(t("search_misc_btn"), key="misc_search"):
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
            st.error(t("query_fail").format(e))
            df2 = pd.DataFrame()

        safe_st_dataframe(df2, height=520)
        if not df2.empty:
            buf2 = io.BytesIO()
            with pd.ExcelWriter(buf2, engine="openpyxl") as writer:
                df2.to_excel(writer, index=False)
            buf2.seek(0)
            st.download_button(t("download_misc"), buf2, "misc_costs.xlsx", key="download_misc")


# ==================== PAGE: ADMIN ====================
if user["role"] == "admin":
    with nav_tabs[3]:
        ui_card(t("admin_panel"), t("admin_panel_sub"))
        ui_hr()

        st.header(t("admin_user_mgmt"))
        users_df = pd.read_sql(text("SELECT id, username, role, region FROM users ORDER BY id"), engine)
        safe_st_dataframe(users_df, height=420)

        ui_hr()
        st.subheader(t("admin_create_user_title"))
        st.caption(t("admin_create_user_sub"))

        with st.form("admin_create_user_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            new_user = c1.text_input(t("admin_create_username"), key="admin_create_username_input")
            new_pass = c2.text_input(t("admin_create_password"), type="password", key="admin_create_password_input")
            new_region_create = c3.selectbox(t("admin_create_region"), REGION_OPTIONS, key="admin_create_region_select")
            confirm_create = st.checkbox(t("admin_create_confirm"), key="admin_create_confirm_check")
            submit_create = st.form_submit_button(t("admin_create_btn"))

        if submit_create:
            if not new_user or not new_pass:
                st.warning(t("empty_user_pass"))
            elif not confirm_create:
                st.warning(t("admin_create_confirm_first"))
            else:
                try:
                    pw_hash = hashlib.sha256(new_pass.encode()).hexdigest()
                    with engine.begin() as conn:
                        conn.execute(
                            text("INSERT INTO users (username,password,role,region) VALUES (:u,:p,'user',:r)"),
                            {"u": new_user, "p": pw_hash, "r": new_region_create}
                        )
                    st.success(t("admin_create_success").format(new_user, new_region_create))
                    safe_rerun()
                except Exception:
                    st.error(t("admin_create_fail"))

        ui_hr()
        st.subheader(t("update_region_title"))

        region_options = REGION_OPTIONS_ADMIN
        user_choices = [f"{row['id']} | {row['username']} | {row['role']} | {row['region']}" for _, row in users_df.iterrows()]

        with st.form("admin_update_user_region_form"):
            target = st.selectbox(t("select_user_update"), user_choices, key="admin_update_user_select")
            new_region = st.selectbox(t("new_region"), region_options, key="admin_update_user_region")
            confirm_update = st.checkbox(t("confirm_update_region"), key="admin_update_user_confirm")
            submit_update = st.form_submit_button(t("update_region_btn"))

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
                        st.warning(t("default_admin_warn"))
                    elif not confirm_update:
                        st.warning(t("confirm_update_first"))
                    else:
                        with engine.begin() as conn:
                            conn.execute(text("UPDATE users SET region=:r WHERE id=:id"), {"r": new_region, "id": target_id})
                        st.success(t("update_region_success").format(target_username, new_region))
                        safe_rerun()
            except Exception as e:
                st.error(t("update_fail").format(e))

        ui_hr()
        st.subheader(t("delete_user_title"))
        st.caption(t("delete_user_caption"))

        protected_usernames = {user["username"], "admin"}
        deletable_rows = users_df[~users_df["username"].isin(protected_usernames)].copy()

        if deletable_rows.empty:
            st.info(t("no_deletable_user"))
        else:
            del_choices = [f"{row['id']} | {row['username']} | {row['role']} | {row['region']}" for _, row in deletable_rows.iterrows()]

            with st.form("admin_delete_users_form"):
                selected = st.multiselect(t("select_delete_user"), del_choices, key="admin_delete_users_select")
                confirm_del = st.checkbox(t("confirm_delete_user"), key="admin_delete_users_confirm")
                submit_del = st.form_submit_button(t("delete_user_btn"))

            if submit_del:
                if not selected:
                    st.warning(t("select_delete_first"))
                elif not confirm_del:
                    st.warning(t("confirm_delete_first"))
                else:
                    try:
                        del_ids = [int(s.split("|", 1)[0].strip()) for s in selected]
                        check_df = users_df[users_df["id"].isin(del_ids)]
                        bad = check_df[check_df["username"].isin(protected_usernames)]
                        if not bad.empty:
                            st.error(t("protected_user_error"))
                        else:
                            placeholders = ",".join(str(i) for i in del_ids)
                            with engine.begin() as conn:
                                conn.execute(text(f"DELETE FROM users WHERE id IN ({placeholders})"))
                            st.success(t("delete_user_success").format(len(del_ids)))
                            safe_rerun()
                    except Exception as e:
                        st.error(t("delete_archive_fail").format(e))
