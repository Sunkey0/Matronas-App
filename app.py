# app.py — Inventario Restaurante "Matronas" (versión avanzada)
# Streamlit + SQLite, costeo con merma/corrección, impuesto 8%, empaque,
# compras por bloques con histórico de precios, importador desde Excel y flujo de caja.
# -----------------------------------------------------------------------------------
# Requisitos: streamlit, pandas, plotly, openpyxl  (sqlite3/zoneinfo/numpy estándar)
# Ejecuta: streamlit run app.py
# -----------------------------------------------------------------------------------

from __future__ import annotations
import os
import io
import uuid
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

TZ = ZoneInfo("America/Bogota")

# =======================
# Configuración de UI
# =======================
st.set_page_config(page_title="Matronas • Inventario", page_icon="🍲", layout="wide")
CSS = """
<style>
#MainMenu {visibility: hidden;}
footer {visibility: hidden;}
.block-container {padding-top: 1rem; padding-bottom: 1rem;}
.stMetric {background: rgba(0,0,0,.03); border-radius: 12px; padding: 12px;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)

# =======================
# DB Helpers & Esquema
# =======================
DB_PATH = os.environ.get("DB_PATH", "matronas_inventory.db")

@st.cache_resource(show_spinner=False)
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def now_str():
    return datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S%z")

def gen_sku(prefix="MAT"):
    return f"{prefix}-{uuid.uuid4().hex[:6]}"

def db_query(sql, params=(), as_df=False):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(sql, params)
    if as_df:
        cols = [d[0] for d in cur.description] if cur.description else []
        return pd.DataFrame(cur.fetchall(), columns=cols)
    conn.commit()
    return cur

def table_has_column(table, column) -> bool:
    info = db_query(f"PRAGMA table_info({table});", as_df=True)
    return not info[info["name"] == column].empty

def init_db():
    # Ítems (insumos)
    db_query("""
    CREATE TABLE IF NOT EXISTS items(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE,
        name TEXT NOT NULL,
        category TEXT,
        base_type TEXT CHECK(base_type IN ('mass','volume','unit')) NOT NULL,
        purchase_unit TEXT NOT NULL,
        content_per_purchase REAL NOT NULL DEFAULT 1.0, -- en base (g/ml/unidad) por unidad de compra
        min_stock_base REAL DEFAULT 0,
        price_purchase REAL DEFAULT 0,                  -- precio por unidad de compra
        active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
    );""")
    # Movimientos
    db_query("""
    CREATE TABLE IF NOT EXISTS movements(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        type TEXT CHECK(type IN ('IN','OUT','ADJ')) NOT NULL,
        qty_base REAL NOT NULL,
        qty_input REAL,
        unit_input TEXT,
        unit_cost_at_time REAL,     -- costo por unidad base (g/ml/u) al momento del movimiento (para IN)
        note TEXT,
        user TEXT,
        created_at TEXT,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
    );""")
    # Platos
    db_query("""
    CREATE TABLE IF NOT EXISTS dishes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT NOT NULL,
        price_sale REAL DEFAULT 0,
        packaging_cost REAL DEFAULT 0,
        price_tax_rate REAL DEFAULT 0.08,  -- 8% por defecto
        price_includes_tax INTEGER DEFAULT 1, -- 1 = precio incluye impuesto
        active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
    );""")
    # Recetas
    db_query("""
    CREATE TABLE IF NOT EXISTS recipe(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dish_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        qty_per_dish_base REAL NOT NULL, -- en base (g/ml/unidad)
        waste_pct REAL DEFAULT 0,        -- 0.05 = 5% merma/pérdidas
        corr_factor REAL DEFAULT 1.0,    -- rendimiento/corrección
        FOREIGN KEY(dish_id) REFERENCES dishes(id) ON DELETE CASCADE,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
        UNIQUE(dish_id, item_id)
    );""")
    # Ventas
    db_query("""
    CREATE TABLE IF NOT EXISTS sales(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dish_id INTEGER NOT NULL,
        qty_dishes REAL NOT NULL,
        price_sale_at_time REAL,
        note TEXT,
        user TEXT,
        created_at TEXT,
        FOREIGN KEY(dish_id) REFERENCES dishes(id) ON DELETE CASCADE
    );""")
    # Histórico de precios (por unidad base)
    db_query("""
    CREATE TABLE IF NOT EXISTS price_history(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        price_per_base REAL NOT NULL,
        recorded_at TEXT,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
    );""")
    # Bloques de compras
    db_query("""
    CREATE TABLE IF NOT EXISTS purchase_batches(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_date TEXT NOT NULL,  -- YYYY-MM-DD
        note TEXT,
        user TEXT,
        created_at TEXT
    );""")
    db_query("""
    CREATE TABLE IF NOT EXISTS purchase_lines(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        qty_input REAL NOT NULL,       -- unidad de compra
        unit_input TEXT,
        qty_base REAL NOT NULL,        -- convertido a base (g/ml/u)
        unit_cost_per_base REAL NOT NULL, -- COP por g/ml/u en el momento
        total_cost REAL NOT NULL,
        provider TEXT,
        FOREIGN KEY(batch_id) REFERENCES purchase_batches(id) ON DELETE CASCADE,
        FOREIGN KEY(item_id)  REFERENCES items(id) ON DELETE CASCADE
    );""")

    # Migraciones suaves si faltan columnas (cuando el repo no trae DB)
    if not table_has_column("movements", "unit_cost_at_time"):
        db_query("ALTER TABLE movements ADD COLUMN unit_cost_at_time REAL;")
    if not table_has_column("recipe", "corr_factor"):
        db_query("ALTER TABLE recipe ADD COLUMN corr_factor REAL DEFAULT 1.0;")
    if not table_has_column("dishes", "packaging_cost"):
        db_query("ALTER TABLE dishes ADD COLUMN packaging_cost REAL DEFAULT 0;")
    if not table_has_column("dishes", "price_tax_rate"):
        db_query("ALTER TABLE dishes ADD COLUMN price_tax_rate REAL DEFAULT 0.08;")
    if not table_has_column("dishes", "price_includes_tax"):
        db_query("ALTER TABLE dishes ADD COLUMN price_includes_tax INTEGER DEFAULT 1;")

init_db()

# =======================
# Utilidades de unidades
# =======================
BASE_UNIT = {"mass":"g", "volume":"ml", "unit":"u"}

AUTO_FACTOR = {
    "mass": {"g":1.0, "kg":1000.0, "mg":0.001},
    "volume": {"ml":1.0, "l":1000.0},
    "unit": {"unidad":1.0, "docena":12.0}
}
# 'paquete' => requiere content_per_purchase > 0 definido por el usuario

def infer_content_per_purchase(base_type:str, purchase_unit:str, given:float|None) -> float:
    if purchase_unit.lower() == "paquete":
        return float(given or 0.0)
    table = AUTO_FACTOR.get(base_type, {})
    if purchase_unit in table:
        return table[purchase_unit]
    return float(given or 0.0)

def to_base_from_input(item_row, qty:float, mode:str) -> float:
    """mode: 'compra' => qty en unidad de compra; 'base' => qty ya está en base."""
    if mode == "base":
        return float(qty)
    cpp = float(item_row["content_per_purchase"] or 0)
    return float(qty) * cpp

def base_to_purchase_units(item_row, qty_base:float) -> float:
    cpp = float(item_row["content_per_purchase"] or 1.0)
    if cpp <= 0:
        return 0.0
    return float(qty_base) / cpp

def price_per_base(item_row) -> float:
    cpp = float(item_row["content_per_purchase"] or 1.0)
    pp = float(item_row["price_purchase"] or 0.0)
    if cpp <= 0:
        return 0.0
    return pp / cpp

def price_per_base_at_time(item_row, unit_cost_per_base: float | None) -> float:
    """Prefiere costo histórico si viene; si no, usa el actual."""
    if unit_cost_per_base is not None and unit_cost_per_base > 0:
        return float(unit_cost_per_base)
    return price_per_base(item_row)

def effective_consumption(qty_base: float, waste_pct: float, corr_factor: float) -> float:
    """
    Consumo efectivo = cantidad * (1 + merma) * corr_factor
    - waste_pct (0.05 = 5% pérdidas)
    - corr_factor (ej.: 1.12 para rendimiento real)
    """
    return float(qty_base) * (1.0 + float(waste_pct or 0.0)) * float(corr_factor or 1.0)

# =======================
# Lecturas cacheadas
# =======================
def invalidate_caches():
    fetch_items.clear()
    fetch_categories.clear()
    stock_all.clear()
    fetch_movements.clear()
    fetch_dishes.clear()
    recipe_of_dish.clear()
    sales_recent.clear()

@st.cache_data(ttl=20)
def fetch_items(search="", category=None, active_only=True):
    q = """
    SELECT id, sku, name, category, base_type, purchase_unit, content_per_purchase,
           min_stock_base, price_purchase, active, created_at, updated_at
    FROM items WHERE 1=1
    """
    params=[]
    if search:
        q += " AND (LOWER(name) LIKE ? OR LOWER(sku) LIKE ?)"
        s = f"%{search.lower()}%"; params += [s,s]
    if category:
        q += " AND category = ?"; params.append(category)
    if active_only:
        q += " AND active = 1"
    q += " ORDER BY name ASC"
    return db_query(q, tuple(params), as_df=True)

@st.cache_data(ttl=20)
def fetch_categories():
    df = db_query("SELECT DISTINCT COALESCE(category,'') AS category FROM items ORDER BY 1;", as_df=True)
    return [c for c in df["category"].tolist() if c]

@st.cache_data(ttl=20)
def stock_all():
    q = """
    SELECT i.*,
           COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty_base
                             WHEN m.type='ADJ' THEN m.qty_base
                             WHEN m.type='OUT' THEN -m.qty_base END),0) AS stock_base
    FROM items i
    LEFT JOIN movements m ON m.item_id = i.id
    GROUP BY i.id
    ORDER BY i.name ASC;
    """
    return db_query(q, as_df=True)

@st.cache_data(ttl=20)
def fetch_movements(days=30, item_id=None):
    since = (datetime.now(tz=TZ) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S%z")
    q = """
    SELECT m.id, m.item_id, i.sku, i.name, i.category, i.base_type, i.purchase_unit,
           m.type, m.qty_base, m.qty_input, m.unit_input, m.unit_cost_at_time,
           m.note, m.user, m.created_at
    FROM movements m
    JOIN items i ON i.id = m.item_id
    WHERE m.created_at >= ?
    """
    params = [since]
    if item_id:
        q += " AND m.item_id = ?"; params.append(item_id)
    q += " ORDER BY m.created_at DESC"
    return db_query(q, tuple(params), as_df=True)

@st.cache_data(ttl=20)
def fetch_dishes(active_only=True):
    q = """SELECT id, code, name, price_sale, packaging_cost, price_tax_rate,
                  price_includes_tax, active, created_at, updated_at
           FROM dishes WHERE 1=1"""
    if active_only:
        q += " AND active=1"
    q += " ORDER BY name ASC"
    return db_query(q, as_df=True)

@st.cache_data(ttl=20)
def recipe_of_dish(dish_id:int):
    q = """
    SELECT r.id, r.dish_id, r.item_id, i.sku, i.name AS item_name, i.base_type,
           r.qty_per_dish_base, r.waste_pct, r.corr_factor,
           i.purchase_unit, i.content_per_purchase, i.price_purchase
    FROM recipe r
    JOIN items i ON i.id = r.item_id
    WHERE r.dish_id = ?
    ORDER BY i.name ASC
    """
    return db_query(q, (dish_id,), as_df=True)

@st.cache_data(ttl=20)
def sales_recent(days=30):
    since = (datetime.now(tz=TZ) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S%z")
    q = """
    SELECT s.id, d.code, d.name AS dish_name, s.qty_dishes, s.price_sale_at_time, s.user, s.note, s.created_at
    FROM sales s
    JOIN dishes d ON d.id = s.dish_id
    WHERE s.created_at >= ?
    ORDER BY s.created_at DESC
    """
    return db_query(q, (since,), as_df=True)

# =======================
# CRUD / acciones
# =======================
def create_item(name, category, base_type, purchase_unit, content_per_purchase,
                min_stock_base, price_purchase, active=True, sku=None):
    ts = now_str()
    if not sku: sku = gen_sku()
    db_query("""INSERT INTO items(sku,name,category,base_type,purchase_unit,content_per_purchase,
                                  min_stock_base,price_purchase,active,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
             (sku, name.strip(), category or None, base_type, purchase_unit,
              float(content_per_purchase or 0), float(min_stock_base or 0),
              float(price_purchase or 0), 1 if active else 0, ts, ts))
    invalidate_caches(); return sku

def update_item(item_id:int, **fields):
    sets, params = [], []
    for k,v in fields.items():
        sets.append(f"{k}=?"); params.append(v)
    sets.append("updated_at=?"); params.append(now_str()); params.append(item_id)
    db_query(f"UPDATE items SET {', '.join(sets)} WHERE id = ?", tuple(params))
    invalidate_caches()

def delete_item(item_id:int):
    db_query("DELETE FROM items WHERE id=?", (item_id,)); invalidate_caches()

def toggle_item_active(item_id:int, active:bool):
    db_query("UPDATE items SET active=?, updated_at=? WHERE id=?", (1 if active else 0, now_str(), item_id)); invalidate_caches()

def add_movement(item_id:int, mtype:str, qty_base:float, qty_input=None, unit_input=None,
                 note:str="", user:str="") -> int:
    if mtype not in ("IN","OUT","ADJ"): raise ValueError("Tipo inválido")
    cur = db_query("""INSERT INTO movements(item_id,type,qty_base,qty_input,unit_input,unit_cost_at_time,note,user,created_at)
                      VALUES(?,?,?,?,?,?,?,?,?)""",
                   (item_id, mtype, float(qty_base),
                    None if qty_input is None else float(qty_input),
                    unit_input, None, note.strip(), user.strip() or "usuario", now_str()))
    invalidate_caches()
    return int(cur.lastrowid)

def create_dish(code, name, price_sale, active=True, packaging_cost:float=0.0,
                price_tax_rate:float=0.08, price_includes_tax:int=1):
    ts = now_str()
    db_query("""INSERT INTO dishes(code,name,price_sale,packaging_cost,price_tax_rate,price_includes_tax,active,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?, ?, ?)""",
             (code.strip() if code else None, name.strip(), float(price_sale or 0),
              float(packaging_cost or 0.0), float(price_tax_rate or 0.08), int(price_includes_tax),
              1 if active else 0, ts, ts))
    invalidate_caches()

def update_dish(dish_id:int, **fields):
    sets, params = [], []
    for k,v in fields.items():
        sets.append(f"{k}=?"); params.append(v)
    sets.append("updated_at=?"); params.append(now_str()); params.append(dish_id)
    db_query(f"UPDATE dishes SET {', '.join(sets)} WHERE id = ?", tuple(params))
    invalidate_caches()

def add_recipe_line(dish_id:int, item_id:int, qty_per_dish_base:float, waste_pct:float, corr_factor:float=1.0):
    db_query("""INSERT OR REPLACE INTO recipe(dish_id,item_id,qty_per_dish_base,waste_pct,corr_factor)
                VALUES(?,?,?,?,?)""",
             (dish_id, item_id, float(qty_per_dish_base), float(waste_pct or 0), float(corr_factor or 1.0)))
    invalidate_caches()

def delete_recipe_line(recipe_id:int):
    db_query("DELETE FROM recipe WHERE id=?", (recipe_id,)); invalidate_caches()

def register_sale(dish_id:int, qty_dishes:float, user:str, note:str):
    dish = db_query("SELECT price_sale FROM dishes WHERE id=?", (dish_id,), as_df=True).iloc[0]
    db_query("""INSERT INTO sales(dish_id,qty_dishes,price_sale_at_time,user,note,created_at)
                VALUES(?,?,?,?,?,?)""",
             (dish_id, float(qty_dishes), float(dish["price_sale"] or 0), user or "usuario", note.strip(), now_str()))
    # Descontar ingredientes según receta
    bom = recipe_of_dish(dish_id)
    for _, r in bom.iterrows():
        need = effective_consumption(float(r["qty_per_dish_base"]), float(r["waste_pct"] or 0.0), float(r.get("corr_factor",1.0))) * float(qty_dishes)
        add_movement(int(r["item_id"]), "OUT", need, None, BASE_UNIT[r["base_type"]],
                     note=f"Consumo por venta de plato (x{qty_dishes})", user=user or "usuario")

# =======================
# Costeo de plato
# =======================
def dish_cost_breakdown(dish_id: int, include_pack: bool = False):
    """
    Retorna dict con:
      costo_insumos, costo_empaque, costo_total, price_sale_bruto, price_neto_sin_impto,
      utilidad, margen_pct, pct_costo, impuesto_pct, incluye_impuesto
    """
    d = db_query("""SELECT id, price_sale, packaging_cost, price_tax_rate, price_includes_tax
                    FROM dishes WHERE id=?""", (dish_id,), as_df=True)
    if d.empty:
        return None
    dish = d.iloc[0]
    bom = recipe_of_dish(dish_id)
    if bom.empty:
        ins = 0.0
    else:
        items = fetch_items(active_only=False).set_index("id")
        bom = bom.merge(items[["price_purchase","content_per_purchase","base_type"]], left_on="item_id", right_index=True, how="left")
        bom["ppb"] = bom.apply(lambda r: (r["price_purchase"] or 0.0) / (r["content_per_purchase"] or 1.0), axis=1)
        bom["consumo"] = bom.apply(lambda r: effective_consumption(r["qty_per_dish_base"], r["waste_pct"], r.get("corr_factor",1.0)), axis=1)
        bom["costo"] = bom["ppb"] * bom["consumo"]
        ins = float(bom["costo"].sum())

    pack = float(dish.get("packaging_cost", 0.0) or 0.0) if include_pack else 0.0
    costo_total = ins + pack

    impto = float(dish.get("price_tax_rate", 0.08) or 0.0)
    incluye = bool(dish.get("price_includes_tax", 1))
    price_bruto = float(dish["price_sale"] or 0.0)
    price_neto = price_bruto / (1.0 + impto) if incluye else price_bruto

    utilidad = price_neto - costo_total
    margen_pct = (utilidad / price_neto) if price_neto > 0 else 0.0
    pct_costo = (costo_total / price_neto) if price_neto > 0 else 0.0

    return {
        "costo_insumos": ins, "costo_empaque": pack, "costo_total": costo_total,
        "price_sale_bruto": price_bruto, "price_neto_sin_impto": price_neto,
        "utilidad": utilidad, "margen_pct": margen_pct, "pct_costo": pct_costo,
        "impuesto_pct": impto, "incluye_impuesto": incluye
    }

def fmt_base(base_type:str) -> str:
    return BASE_UNIT.get(base_type, "")

# =======================
# Páginas
# =======================
def page_dashboard():
    st.subheader("📊 Dashboard Matronas")
    data = stock_all()
    if data.empty:
        st.info("Aún no hay ítems. Crea algunos en **Ítems**.")
        return
    total_activos = int((data["active"]==1).sum())
    bajos = data[(data["active"]==1) & (data["stock_base"] < data["min_stock_base"])]

    tmp = data.copy()
    tmp["ppb"] = tmp.apply(price_per_base, axis=1)
    inv_value = float((tmp["ppb"] * tmp["stock_base"]).sum())

    c1,c2,c3 = st.columns(3)
    c1.metric("Ítems activos", f"{total_activos}")
    c2.metric("Valor inventario (COP)", f"{inv_value:,.0f}")
    c3.metric("Bajo stock", f"{len(bajos)}")

    st.divider()
    left,right = st.columns([1,1])
    with left:
        st.markdown("**Bajo mínimo**")
        if bajos.empty:
            st.success("Todo en niveles adecuados.")
        else:
            df_show = bajos.copy()
            df_show["stock (base)"] = df_show.apply(lambda r: f"{r['stock_base']:.0f} {fmt_base(r['base_type'])}", axis=1)
            df_show["min (base)"] = df_show.apply(lambda r: f"{r['min_stock_base']:.0f} {fmt_base(r['base_type'])}", axis=1)
            df_show["stock (compra)"] = df_show.apply(lambda r: f"{base_to_purchase_units(r, r['stock_base']):.2f} {r['purchase_unit']}", axis=1)
            st.dataframe(df_show[["sku","name","category","stock (base)","min (base)","stock (compra)","price_purchase"]],
                         use_container_width=True, hide_index=True)
    with right:
        st.markdown("**Stock por categoría (base)**")
        agg = data.groupby("category", dropna=False)["stock_base"].sum().reset_index()
        fig = px.bar(agg, x="category", y="stock_base", title="Suma de stock (base)")
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("**Movimientos últimos 30 días**")
    mv = fetch_movements(days=30)
    st.dataframe(mv, use_container_width=True, hide_index=True)

def page_items():
    st.subheader("🗂️ Ítems (insumos)")
    preset_cats = ["Bebidas","Verduras","Salsas","Carnes","Lácteos","Granos","Desechables","Limpieza"]
    base_types = {"Masa":"mass","Volumen":"volume","Unidad":"unit"}
    units_by_base = {
        "mass": ["g","kg","mg","paquete"],
        "volume": ["ml","l","paquete"],
        "unit": ["unidad","docena","paquete"]
    }

    tab1, tab2 = st.tabs(["Crear/Editar", "Listado y exportación"])
    with tab1:
        st.markdown("**Crear ítem**")

        # Selector reactivo FUERA del form (para cambiar unidades)
        if "base_lbl_create" not in st.session_state:
            st.session_state.base_lbl_create = "Volumen"
        col_base, col_info = st.columns([1,2])
        with col_base:
            st.session_state.base_lbl_create = st.selectbox(
                "Tipo base*", list(base_types.keys()),
                index=["Masa","Volumen","Unidad"].index(st.session_state.base_lbl_create),
                key="base_lbl_create_select"
            )
        base_type = base_types[st.session_state.base_lbl_create]
        with col_info:
            st.caption(f"Unidad base: **{fmt_base(base_type)}** — las opciones de compra dependen de esto.")

        with st.form("item_form", clear_on_submit=True):
            c1,c2,c3,c4 = st.columns(4)
            name = c1.text_input("Nombre*", placeholder="Tomate Chonto", key="name_create")
            category = c2.selectbox("Categoría", ["(escribe otra)"] + preset_cats, index=2, key="category_create")
            category = None if category=="(escribe otra)" else category
            purchase_unit = c4.selectbox("Unidad de compra*", units_by_base[base_type], key="purchase_unit_create")

            c5,c6,c7,c8 = st.columns(4)
            given_cpp = c5.number_input(
                f"Contenido por unidad de compra (en {fmt_base(base_type)})",
                min_value=0.0, value=0.0, step=1.0, key="cpp_create",
                help="Ej.: paquete=500 g → escribe 500. kg → deja 0 (se infiere 1000 g)"
            )
            inferred = infer_content_per_purchase(base_type, purchase_unit, given_cpp)
            c6.metric("Factor inferido", f"{inferred:.2f} {fmt_base(base_type)}")
            min_stock = c7.number_input(f"Stock mínimo (en {fmt_base(base_type)})", min_value=0.0, value=0.0, step=1.0, key="min_stock_create")
            price_purchase = c8.number_input("Precio por unidad de compra (COP)", min_value=0.0, value=0.0, step=100.0, key="price_create")
            c9,c10 = st.columns(2)
            sku_custom = c9.text_input("SKU (opcional)", key="sku_create")
            active = c10.checkbox("Activo", value=True, key="active_create")

            submit = st.form_submit_button("Guardar", use_container_width=True)
            if submit:
                if not name.strip():
                    st.error("Nombre es obligatorio.")
                elif purchase_unit=="paquete" and inferred<=0:
                    st.error("Define el contenido del paquete en unidades base (>0).")
                else:
                    try:
                        sku = create_item(
                            name=name, category=category, base_type=base_type,
                            purchase_unit=purchase_unit, content_per_purchase=inferred,
                            min_stock_base=min_stock, price_purchase=price_purchase,
                            active=active, sku=sku_custom or None
                        )
                        st.success(f"Ítem creado (SKU: {sku}).")
                    except sqlite3.IntegrityError:
                        st.error("SKU duplicado.")

        st.divider()
        st.markdown("**Editar ítem**")
        df = fetch_items(active_only=False)
        if df.empty:
            st.info("No hay ítems aún.")
        else:
            sel = st.selectbox("Selecciona", df["name"] + " — " + df["sku"], key="select_edit_item")
            row = df.loc[df["name"] + " — " + df["sku"] == sel].iloc[0]
            with st.form("edit_form"):
                c1,c2,c3,c4 = st.columns(4)
                name_e = c1.text_input("Nombre*", value=row["name"], key="name_edit")
                category_e = c2.text_input("Categoría", value=row["category"] or "", key="category_edit")
                base_type_e = c3.selectbox("Tipo base (edición)*", ["mass","volume","unit"],
                                           index=["mass","volume","unit"].index(row["base_type"]), key="base_type_edit")
                purchase_unit_e = c4.text_input("Unidad de compra (edición)*", value=row["purchase_unit"], key="purchase_unit_edit")
                c5,c6,c7,c8 = st.columns(4)
                cpp_e = c5.number_input(f"Contenido por unidad de compra ({fmt_base(base_type_e)})",
                                        min_value=0.0, value=float(row["content_per_purchase"] or 0), step=1.0, key="cpp_edit")
                min_stock_e = c6.number_input(f"Mínimo ({fmt_base(base_type_e)})",
                                              min_value=0.0, value=float(row["min_stock_base"] or 0), step=1.0, key="min_stock_edit")
                price_e = c7.number_input("Precio unidad de compra (COP)", min_value=0.0,
                                          value=float(row["price_purchase"] or 0), step=100.0, key="price_edit")
                sku_e = c8.text_input("SKU", value=row["sku"], key="sku_edit")
                cA,cB,cC = st.columns(3)
                upd = cA.form_submit_button("Actualizar")
                toggle = cB.form_submit_button("Activar/Desactivar")
                delete = cC.form_submit_button("Eliminar")
                if upd:
                    try:
                        update_item(int(row["id"]),
                                    name=name_e.strip(),
                                    category=(category_e.strip() or None),
                                    base_type=base_type_e,
                                    purchase_unit=purchase_unit_e.strip(),
                                    content_per_purchase=float(cpp_e),
                                    min_stock_base=float(min_stock_e),
                                    price_purchase=float(price_e),
                                    sku=sku_e.strip())
                        st.success("Ítem actualizado.")
                    except sqlite3.IntegrityError:
                        st.error("SKU duplicado.")
                if toggle:
                    toggle_item_active(int(row["id"]), not bool(row["active"]))
                    st.success("Estado cambiado.")
                if delete:
                    delete_item(int(row["id"]))
                    st.success("Ítem eliminado.")

    with tab2:
        st.markdown("**Listado y exportación**")
        cats = ["(todas)"] + fetch_categories()
        c1,c2,c3 = st.columns([2,1,1])
        q = c1.text_input("Buscar por nombre/SKU")
        cat = c2.selectbox("Categoría", cats)
        active_only = c3.toggle("Solo activos", value=True)
        df = fetch_items(q, None if cat=="(todas)" else cat, active_only)
        if not df.empty:
            st.caption("Las cantidades se muestran en base y en unidades de compra.")
            joined = df.merge(stock_all()[["id","stock_base"]], on="id", how="left")
            joined["stock_base_str"] = joined.apply(lambda r: f"{r['stock_base']:.0f} {fmt_base(r['base_type'])}", axis=1)
            joined["stock_compra_str"] = joined.apply(lambda r: f"{base_to_purchase_units(r, r['stock_base']):.2f} {r['purchase_unit']}", axis=1)
            joined["precio_base"] = joined.apply(price_per_base, axis=1)
            st.dataframe(joined[["sku","name","category","base_type","purchase_unit",
                                 "stock_base_str","stock_compra_str","min_stock_base","price_purchase","precio_base"]],
                         use_container_width=True, hide_index=True)
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as w:
                joined.to_excel(w, index=False, sheet_name="items")
            st.download_button("⬇️ Exportar Excel", data=out.getvalue(),
                               file_name="matronas_items.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)

def page_movements():
    st.subheader("↕️ Movimientos (compras, consumos, ajustes)")
    df_items = fetch_items(active_only=False)
    if df_items.empty:
        st.info("Primero crea ítems.")
        return
    c1,c2,c3,c4 = st.columns([2,1,1,1])
    sel = c1.selectbox("Ítem", df_items["name"] + " — " + df_items["sku"])
    row = df_items.loc[df_items["name"] + " — " + df_items["sku"] == sel].iloc[0]
    base_u = fmt_base(row["base_type"])
    mtype = c2.selectbox("Tipo", ["IN","OUT","ADJ"])
    mode = c3.selectbox("Cantidad ingresada en", ["compra","base"], help="‘compra’ usa la unidad de compra (kg/l/paquete/unidad).")
    qty_in = c4.number_input("Cantidad", min_value=0.0, value=0.0, step=1.0)
    note = st.text_input("Nota", value="Movimiento manual")
    user = st.text_input("Usuario", value="usuario")
    if st.button("Registrar movimiento", use_container_width=True):
        if qty_in <= 0:
            st.error("Cantidad debe ser > 0.")
        else:
            qbase = to_base_from_input(row, qty_in, mode)
            mid = add_movement(int(row["id"]), mtype, qbase, qty_input=qty_in,
                               unit_input=(row["purchase_unit"] if mode=="compra" else base_u),
                               note=note, user=user)
            # si es IN, guardamos costo histórico usando precio actual
            if mtype == "IN":
                unit_cost = price_per_base(row)
                db_query("UPDATE movements SET unit_cost_at_time=? WHERE id=?", (unit_cost, int(mid)))
            st.success(f"Movimiento registrado ({mtype}).")

    st.divider()
    st.markdown("**Historial**")
    c5,c6 = st.columns(2)
    days = c5.slider("Rango (días)", min_value=7, max_value=180, value=30)
    only_this = c6.checkbox("Solo del ítem seleccionado", value=False)
    item_id = int(row["id"]) if only_this else None
    df = fetch_movements(days=days, item_id=item_id)
    st.dataframe(df, use_container_width=True, hide_index=True)

def page_count():
    st.subheader("🧮 Conteo físico")
    base = stock_all()
    if base.empty:
        st.info("No hay ítems.")
        return
    base = base[["id","sku","name","category","base_type","purchase_unit","content_per_purchase","stock_base"]].copy()
    base["conteo_base"] = base["stock_base"]
    st.caption("Edita ‘conteo_base’ (en unidades base: g/ml/u). ‘Aplicar ajustes’ creará movimientos ADJ.")
    edited = st.data_editor(
        base,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        column_config={
            "conteo_base": st.column_config.NumberColumn(f"Conteo ({fmt_base('mass')}/{fmt_base('volume')}/{fmt_base('unit')})", step=1.0)
        }
    )
    edited["dif_base"] = edited["conteo_base"] - edited["stock_base"]
    st.divider()
    st.dataframe(edited[["sku","name","stock_base","conteo_base","dif_base"]], use_container_width=True, hide_index=True)
    note = st.text_input("Nota ajuste", value="Ajuste por conteo físico")
    user = st.text_input("Usuario", value="usuario")
    if st.button("Aplicar ajustes", disabled=edited[edited["dif_base"]!=0].empty, use_container_width=True):
        to_adj = edited[edited["dif_base"]!=0]
        for _, r in to_adj.iterrows():
            add_movement(int(r["id"]), "ADJ", float(r["dif_base"]), None, fmt_base(r["base_type"]), note, user)
        st.success(f"Ajustes aplicados: {len(to_adj)}")
    out = io.BytesIO()
    export_cols = ["sku","name","category","base_type","purchase_unit","content_per_purchase","stock_base","conteo_base","dif_base"]
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        edited[export_cols].to_excel(w, index=False, sheet_name="conteo")
    st.download_button("⬇️ Exportar hoja de conteo (Excel)", data=out.getvalue(),
                       file_name="conteo_matronas.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       use_container_width=True)

def page_dishes():
    st.subheader("🍽️ Platos & Recetas")
    tabs = st.tabs(["Platos","Recetas (BOM)","Costeo"])

    # ---- Platos
    with tabs[0]:
        with st.form("dish_form", clear_on_submit=True):
            c1,c2,c3,c4 = st.columns(4)
            code = c1.text_input("Código (opcional)")
            name = c2.text_input("Nombre del plato*", placeholder="Bandeja Matrona")
            price = c3.number_input("Precio venta (COP, bruto)", min_value=0.0, value=0.0, step=100.0)
            pack = c4.number_input("Costo empaque (COP)", min_value=0.0, value=0.0, step=100.0)
            save = st.form_submit_button("Crear plato")
            if save:
                if not name.strip():
                    st.error("Nombre es obligatorio.")
                else:
                    try:
                        create_dish(code or None, name, price, active=True, packaging_cost=pack)
                        st.success("Plato creado.")
                    except sqlite3.IntegrityError:
                        st.error("Código de plato duplicado.")
        st.divider()
        d = fetch_dishes(active_only=False)
        if d.empty:
            st.info("No hay platos.")
        else:
            sel = st.selectbox("Selecciona plato", d["name"])
            row = d.loc[d["name"] == sel].iloc[0]
            with st.form("dish_edit"):
                c1,c2,c3,c4 = st.columns(4)
                code_e = c1.text_input("Código", value=row["code"] or "")
                name_e = c2.text_input("Nombre*", value=row["name"])
                price_e = c3.number_input("Precio venta (COP, bruto)", min_value=0.0, value=float(row["price_sale"] or 0), step=100.0)
                active_e = c4.checkbox("Activo", value=bool(row["active"]))
                c5,c6,c7 = st.columns(3)
                pack_e = c5.number_input("Costo empaque (COP)", min_value=0.0, value=float(row.get("packaging_cost",0.0) or 0.0), step=100.0)
                tax_e = c6.number_input("Impuesto consumo (%)", min_value=0.0, max_value=30.0, value=float(row.get("price_tax_rate",0.08))*100, step=0.5)/100.0
                incl_e = c7.checkbox("Precio incluye impuesto", value=bool(row.get("price_includes_tax",1)))
                cA,cB = st.columns(2)
                upd = cA.form_submit_button("Actualizar")
                if upd:
                    update_dish(int(row["id"]), code=(code_e.strip() or None), name=name_e.strip(),
                                price_sale=float(price_e), active=1 if active_e else 0,
                                packaging_cost=float(pack_e), price_tax_rate=float(tax_e),
                                price_includes_tax=(1 if incl_e else 0))
                    st.success("Plato actualizado.")

    # ---- Recetas
    with tabs[1]:
        d = fetch_dishes()
        if d.empty:
            st.info("Crea al menos un plato.")
        else:
            dish_name = st.selectbox("Plato", d["name"], key="rec_dish")
            dish_id = int(d.loc[d["name"]==dish_name, "id"].iloc[0])
            st.markdown("**Añadir ingrediente**")
            items = fetch_items(active_only=True)
            if items.empty:
                st.info("Crea ítems primero.")
            else:
                col1,col2,col3,col4 = st.columns([2,1,1,1])
                item_sel = col1.selectbox("Ítem", items["name"] + " — " + items["sku"])
                item_row = items.loc[items["name"] + " — " + items["sku"] == item_sel].iloc[0]
                qty = col2.number_input(f"Cantidad por porción (en {fmt_base(item_row['base_type'])})", min_value=0.0, value=0.0, step=1.0)
                waste = col3.number_input("Merma %", min_value=0.0, max_value=100.0, value=0.0, step=1.0)
                corr = col4.number_input("Factor corrección", min_value=0.5, max_value=3.0, value=1.0, step=0.01,
                                          help="Rendimiento real: p. ej. 1.12 para pelado/lavado.")
                if st.button("Agregar/Actualizar ingrediente", use_container_width=True):
                    add_recipe_line(dish_id, int(item_row["id"]), qty, waste/100.0, corr)
                    st.success("Ingrediente guardado.")
            st.divider()
            bom = recipe_of_dish(dish_id)
            if bom.empty:
                st.info("Este plato aún no tiene ingredientes.")
            else:
                bom_show = bom.copy()
                bom_show["unidad_base"] = bom_show["base_type"].map(BASE_UNIT)
                # costo unitario base actual
                items = fetch_items(active_only=False).set_index("id")
                bom_show = bom_show.merge(items[["price_purchase","content_per_purchase"]],
                                          left_on="item_id", right_index=True, how="left")
                bom_show["costo_unit_base"] = bom_show.apply(lambda r: (r["price_purchase"] or 0.0)/(r["content_per_purchase"] or 1.0), axis=1)
                bom_show["consumo"] = bom_show.apply(lambda r: effective_consumption(r["qty_per_dish_base"], r["waste_pct"], r.get("corr_factor",1.0)), axis=1)
                bom_show["costo_por_plato"] = bom_show["consumo"] * bom_show["costo_unit_base"]
                st.dataframe(bom_show[["item_name","qty_per_dish_base","unidad_base","waste_pct","corr_factor","consumo","costo_unit_base","costo_por_plato"]],
                             use_container_width=True, hide_index=True)
                # eliminar líneas
                del_id = st.selectbox("Eliminar ingrediente (opcional)", [""] + [f"{r['id']} — {r['item_name']}" for _, r in bom_show.iterrows()])
                if del_id:
                    rid = int(del_id.split(" — ")[0])
                    if st.button("Eliminar seleccionado"):
                        delete_recipe_line(rid)
                        st.success("Ingrediente eliminado.")

    # ---- Costeo
    with tabs[2]:
        d = fetch_dishes()
        if d.empty:
            st.info("Crea platos primero.")
        else:
            dish_name = st.selectbox("Plato a costear", d["name"], key="cost_dish")
            dish = d.loc[d["name"]==dish_name].iloc[0]
            include_pack = st.toggle("Para llevar (sumar empaque)", value=False)
            tax_col1, tax_col2 = st.columns(2)
            tax_rate = tax_col1.number_input("Impuesto al consumo (%)", min_value=0.0, max_value=30.0,
                                             value=float(dish.get("price_tax_rate",0.08))*100, step=0.5) / 100.0
            incl_tax = tax_col2.checkbox("Precio incluye impuesto", value=bool(dish.get("price_includes_tax",1)))
            if st.button("Guardar parámetros de impuesto", use_container_width=True):
                update_dish(int(dish["id"]), price_tax_rate=float(tax_rate), price_includes_tax=(1 if incl_tax else 0))
                st.success("Parámetros actualizados.")

            # Editor rápido de merma y factor
            bom = recipe_of_dish(int(dish["id"]))
            if bom.empty:
                st.warning("Agrega ingredientes a la receta para ver el costeo.")
            else:
                bom_edit = bom.copy()
                bom_edit["merma_%"] = (bom_edit["waste_pct"].fillna(0.0) * 100).round(2)
                if "corr_factor" not in bom_edit.columns: bom_edit["corr_factor"] = 1.0
                edited = st.data_editor(
                    bom_edit[["id","item_name","qty_per_dish_base","merma_%","corr_factor"]],
                    hide_index=True,
                    column_config={
                        "qty_per_dish_base": st.column_config.NumberColumn("Cantidad base", step=1.0),
                        "merma_%": st.column_config.NumberColumn("Merma %", min_value=0.0, max_value=100.0, step=0.5),
                        "corr_factor": st.column_config.NumberColumn("Factor corrección", min_value=0.5, max_value=3.0, step=0.01),
                    }
                )
                if st.button("Guardar receta (mermas y factores)", use_container_width=True):
                    for _, r in edited.iterrows():
                        db_query("UPDATE recipe SET qty_per_dish_base=?, waste_pct=?, corr_factor=? WHERE id=?",
                                 (float(r["qty_per_dish_base"]), float(r["merma_%"]/100.0), float(r["corr_factor"]), int(r["id"])))
                    st.success("Receta actualizada.")

                # Recalcular y mostrar métricas finales
                bom2 = recipe_of_dish(int(dish["id"]))
                items = fetch_items(active_only=False).set_index("id")
                bom2 = bom2.merge(items[["price_purchase","content_per_purchase","base_type"]], left_on="item_id", right_index=True, how="left")
                bom2["ppb"] = bom2.apply(lambda r: (r["price_purchase"] or 0.0) / (r["content_per_purchase"] or 1.0), axis=1)
                bom2["consumo"] = bom2.apply(lambda r: effective_consumption(r["qty_per_dish_base"], r["waste_pct"], r.get("corr_factor",1.0)), axis=1)
                bom2["costo_ingrediente"] = bom2["ppb"] * bom2["consumo"]

                costo_ins = float(bom2["costo_ingrediente"].sum())
                pack = float(dish.get("packaging_cost",0.0) or 0.0) if include_pack else 0.0
                price_bruto = float(dish["price_sale"] or 0.0)
                price_neto = price_bruto / (1.0 + tax_rate) if incl_tax else price_bruto
                utilidad = price_neto - (costo_ins + pack)
                margen = (utilidad/price_neto) if price_neto>0 else 0.0
                pct_costo = ((costo_ins + pack) / price_neto) if price_neto>0 else 0.0

                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Costo insumos", f"{costo_ins:,.0f} COP")
                c2.metric("Empaque", f"{pack:,.0f}")
                c3.metric("Margen", f"{margen*100:.1f}%")
                c4.metric("Costo/Precio", f"{pct_costo*100:.1f}%")

                st.dataframe(bom2[["item_name","qty_per_dish_base","waste_pct","corr_factor","consumo","ppb","costo_ingrediente"]],
                             use_container_width=True, hide_index=True)

                objetivo = st.slider("Margen objetivo (%)", 20, 80, 60) / 100.0
                precio_neto_sugerido = (costo_ins + pack) / (1.0 - objetivo) if objetivo<1 else 0.0
                precio_final_sugerido = precio_neto_sugerido * (1.0 + tax_rate) if incl_tax else precio_neto_sugerido
                st.info(f"Precio sugerido (final): **{precio_final_sugerido:,.0f} COP**  | Neto sin impuesto: {precio_neto_sugerido:,.0f}")

def page_sales():
    st.subheader("🧾 Ventas (descuento automático de insumos)")
    dishes = fetch_dishes()
    if dishes.empty:
        st.info("Crea platos y su receta primero.")
        return
    dish_name = st.selectbox("Plato vendido", dishes["name"])
    dish_id = int(dishes.loc[dishes["name"]==dish_name, "id"].iloc[0])
    qty = st.number_input("Cantidad de porciones", min_value=1.0, value=1.0, step=1.0)
    note = st.text_input("Nota", value="Venta registrada")
    user = st.text_input("Usuario", value="usuario")
    if st.button("Registrar venta y descontar insumos", use_container_width=True):
        bom = recipe_of_dish(dish_id)
        if bom.empty:
            st.error("El plato no tiene receta configurada.")
        else:
            register_sale(dish_id, qty, user, note)
            st.success("Venta registrada y consumo descontado.")
    st.divider()
    st.markdown("**Ventas recientes**")
    df = sales_recent(days=30)
    st.dataframe(df, use_container_width=True, hide_index=True)

def page_purchases():
    st.subheader("🛒 Compras por día (bloques)")
    batch_date = st.date_input("Fecha de compra", value=datetime.now(tz=TZ).date())
    note = st.text_input("Nota", value=f"Compras {batch_date.isoformat()}")
    user = st.text_input("Usuario", value="usuario")

    items = fetch_items(active_only=True)
    if items.empty:
        st.info("Primero crea ítems.")
        return

    template = items[["id","name","purchase_unit"]].copy()
    template = template.rename(columns={"id":"item_id","name":"item"})
    template["cantidad_compra"] = 0.0
    template["precio_unidad_compra"] = 0.0

    edit = st.data_editor(
        template[["item_id","item","purchase_unit","cantidad_compra","precio_unidad_compra"]],
        hide_index=True, num_rows="dynamic",
        column_config={
            "item_id": st.column_config.NumberColumn("ID", disabled=True),
            "item": st.column_config.TextColumn("Ítem", disabled=True),
            "purchase_unit": st.column_config.TextColumn("Unidad compra", disabled=True),
            "cantidad_compra": st.column_config.NumberColumn("Cantidad (unidad de compra)", min_value=0.0, step=1.0),
            "precio_unidad_compra": st.column_config.NumberColumn("Precio por unidad de compra (COP)", min_value=0.0, step=100.0),
        }
    )

    if st.button("Registrar bloque de compras", use_container_width=True):
        ts = now_str()
        db_query("""INSERT INTO purchase_batches(batch_date, note, user, created_at)
                    VALUES(?,?,?,?)""", (batch_date.isoformat(), note, user, ts))
        batch_id = db_query("SELECT last_insert_rowid() AS id", as_df=True).iloc[0,0]

        total = 0.0
        for _, r in edit.iterrows():
            iid = int(r["item_id"])
            qty_c = float(r["cantidad_compra"] or 0.0)
            price_pc = float(r["precio_unidad_compra"] or 0.0)
            if iid<=0 or qty_c<=0 or price_pc<=0:
                continue
            row = items[items["id"]==iid].iloc[0]
            cpp = float(row["content_per_purchase"] or 1.0)
            qty_base = qty_c * cpp
            unit_cost_per_base = price_pc / cpp
            total_cost = qty_base * unit_cost_per_base
            total += total_cost

            db_query("""INSERT INTO purchase_lines(batch_id,item_id,qty_input,unit_input,qty_base,unit_cost_per_base,total_cost,provider)
                        VALUES(?,?,?,?,?,?,?,?)""",
                     (batch_id, iid, qty_c, row["purchase_unit"], qty_base, unit_cost_per_base, total_cost, None))
            db_query("""INSERT INTO price_history(item_id, price_per_base, recorded_at)
                        VALUES(?,?,?)""", (iid, unit_cost_per_base, ts))

            # Actualiza precio actual del ítem (precio por unidad de compra)
            db_query("UPDATE items SET price_purchase=?, updated_at=? WHERE id=?",
                     (price_pc, ts, iid))

            # Movimiento IN con costo histórico preciso
            mid = add_movement(iid, "IN", qty_base, qty_input=qty_c, unit_input=row["purchase_unit"],
                               note=f"Compra batch {batch_id}", user=user)
            db_query("UPDATE movements SET unit_cost_at_time=? WHERE id=?", (unit_cost_per_base, int(mid)))

        st.success(f"Compras registradas. Total aprox: {total:,.0f} COP")

        hist = db_query("""
            SELECT i.name, MAX(ph.recorded_at) AS last_ts, ph.price_per_base
            FROM price_history ph JOIN items i ON i.id=ph.item_id
            WHERE ph.recorded_at = (SELECT MAX(recorded_at) FROM price_history WHERE item_id=ph.item_id)
            GROUP BY ph.item_id
        """, as_df=True)
        st.caption("Últimos precios por base (aprox)")
        st.dataframe(hist, use_container_width=True, hide_index=True)

def page_import():
    st.subheader("📥 Importar costeo desde Excel (plantilla)")
    up = st.file_uploader("Sube tu Excel (.xlsx) — hoja 'Platos'", type=["xlsx"])
    if not up:
        st.info("Usa la plantilla de costeo que compartiste (hoja **Platos**).")
        return

    def base_type_from_unit(u:str):
        u = str(u).strip().lower()
        if u in ["gr","g","gramos"]:   return "mass","g"
        if u in ["ml","mililitros"]:   return "volume","ml"
        if u in ["und","unidad","u","0","unidades"]: return "unit","unidad"
        return "unit","unidad"

    try:
        xls = pd.ExcelFile(up)
        df = pd.read_excel(xls, "Platos", header=None)
    except Exception as e:
        st.error(f"No pude leer la hoja 'Platos': {e}")
        return

    # localizar bloques por "PLATO" en fila 0
    blocks, c = [], 0
    while c < df.shape[1]:
        if str(df.iat[0,c]).strip().upper()=="PLATO":
            s=c; c2=c+1
            while c2<df.shape[1] and str(df.iat[0,c2]).strip().upper()!="PLATO":
                c2+=1
            blocks.append((s,c2)); c=c2
        else:
            c+=1

    items, dishes, recipes = {}, [], []
    for (s,e) in blocks:
        block = df.iloc[:, s:e]
        R = block.shape[0]
        # nombre
        name=None
        for r in range(0, min(10, R-1)):
            if str(block.iat[r,0]).strip().lower()=="nombre" and r+1<R:
                nm = str(block.iat[r+1,0]).strip()
                if nm and nm.lower()!="nan":
                    name=nm; break
        if not name: continue
        # precio de venta (mesa)
        price_sale=None
        for r in range(0, min(12, R-1)):
            for cc in range(block.shape[1]):
                if str(block.iat[r,cc]).strip().lower()=="precio de venta al público":
                    v = block.iat[r+1,cc]
                    if pd.notna(v):
                        price_sale=float(v); break
            if price_sale is not None: break
        # insumos
        insumo_row=None
        for r in range(10, min(40, R)):
            rowvals=[str(x).strip().lower() for x in block.iloc[r,:].tolist()]
            if "insumos" in rowvals and "cantidad estándar" in rowvals and "costo real" in rowvals:
                insumo_row=r; break
        if insumo_row is not None:
            header=[str(x).strip().lower() for x in block.iloc[insumo_row,:].tolist()]
            idx_ing = header.index("insumos") if "insumos" in header else 0
            idx_qty = header.index("cantidad estándar") if "cantidad estándar" in header else idx_ing+1
            idx_unit = header.index("und") if "und" in header else idx_qty+1
            idx_cost = header.index("costo real") if "costo real" in header else idx_unit+1
            r = insumo_row+1
            while r < R:
                ing = block.iat[r, idx_ing]
                if pd.isna(ing) or str(ing).strip()=="" or str(ing).strip().lower() in ["mesa","domicilio"]:
                    break
                try: qty=float(block.iat[r, idx_qty])
                except: qty=None
                unit=str(block.iat[r, idx_unit]).strip().lower()
                try: cost=float(block.iat[r, idx_cost])
                except: cost=0.0
                base_type, purchase_unit = base_type_from_unit(unit)
                key=str(ing).strip()
                if key not in items:
                    items[key]={"name":key,"base_type":base_type,"purchase_unit":purchase_unit,
                                "content_per_purchase":1.0,"price_purchase":float(cost),"category":None}
                if qty is not None:
                    recipes.append({"dish_name":name,"item_name":key,"qty_per_dish_base":qty,"base_type":base_type})
                r+=1
        # merma % uniforme
        margen_abs, costo_total = None, None
        for r in range(11, min(25, R-1)):
            for cc in range(block.shape[1]):
                cell=str(block.iat[r,cc]).strip().lower()
                if cell=="costo total de ins.":
                    v=block.iat[r+1,cc]; 
                    if pd.notna(v): costo_total=float(v)
                if cell=="margen de error":
                    v=block.iat[r+1,cc];
                    if pd.notna(v): margen_abs=float(v)
        waste_pct=(margen_abs/costo_total) if (margen_abs and costo_total and costo_total>0) else 0.0
        dishes.append({"name":name,"price_sale":float(price_sale or 0.0),"waste_pct_uniform":waste_pct})

    df_items = pd.DataFrame(list(items.values())).sort_values("name").reset_index(drop=True)
    df_dishes = pd.DataFrame(dishes).drop_duplicates(subset=["name"]).reset_index(drop=True)
    df_recipes = pd.DataFrame(recipes)
    if not df_recipes.empty:
        wmap=dict(zip(df_dishes["name"], df_dishes["waste_pct_uniform"].fillna(0.0)))
        df_recipes["waste_pct"]=df_recipes["dish_name"].map(wmap).fillna(0.0)

    st.success(f"Detectados: {len(df_dishes)} platos, {len(df_items)} ítems, {len(df_recipes)} líneas receta.")
    st.dataframe(df_dishes[["name","price_sale","waste_pct_uniform"]].head(20), use_container_width=True)
    st.dataframe(df_items.head(20), use_container_width=True)
    st.dataframe(df_recipes.head(30), use_container_width=True)

    if st.button("Importar al inventario", use_container_width=True):
        ts = now_str()
        # upsert ítems
        for _, r in df_items.iterrows():
            name=r["name"].strip()
            row=db_query("SELECT id FROM items WHERE name=?", (name,), as_df=True)
            if not row.empty:
                db_query("""UPDATE items SET base_type=?, purchase_unit=?, content_per_purchase=?, price_purchase=?, updated_at=? WHERE id=?""",
                         (r["base_type"], r["purchase_unit"], float(r["content_per_purchase"]), float(r["price_purchase"] or 0.0), ts, int(row.iloc[0]["id"])))
            else:
                db_query("""INSERT INTO items(sku,name,category,base_type,purchase_unit,content_per_purchase,min_stock_base,price_purchase,active,created_at,updated_at)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                         (gen_sku(), name, None, r["base_type"], r["purchase_unit"], float(r["content_per_purchase"]), 0.0, float(r["price_purchase"] or 0.0), 1, ts, ts))
        # upsert platos
        for _, r in df_dishes.iterrows():
            name=r["name"].strip()
            row=db_query("SELECT id FROM dishes WHERE name=?", (name,), as_df=True)
            if row.empty:
                db_query("""INSERT INTO dishes(code,name,price_sale,active,created_at,updated_at,packaging_cost)
                            VALUES(?,?,?,?,?,?,?)""",
                         (None, name, float(r.get("price_sale",0.0) or 0.0), 1, ts, ts, 0.0))
            else:
                db_query("UPDATE dishes SET price_sale=?, updated_at=? WHERE id=?",
                         (float(r.get("price_sale",0.0) or 0.0), ts, int(row.iloc[0]["id"])))
        # map ids
        items_map = db_query("SELECT id,name FROM items", as_df=True).set_index("name")["id"].to_dict()
        dishes_map = db_query("SELECT id,name FROM dishes", as_df=True).set_index("name")["id"].to_dict()
        # limpiar recetas del plato e insertar
        for dname, did in dishes_map.items():
            db_query("DELETE FROM recipe WHERE dish_id=?", (did,))
        for _, r in df_recipes.iterrows():
            did = dishes_map.get(str(r["dish_name"]).strip()); iid = items_map.get(str(r["item_name"]).strip())
            if did and iid:
                db_query("INSERT INTO recipe(dish_id,item_id,qty_per_dish_base,waste_pct,corr_factor) VALUES(?,?,?,?,?)",
                         (int(did), int(iid), float(r["qty_per_dish_base"] or 0.0), float(r.get("waste_pct",0.0) or 0.0), 1.0))
        invalidate_caches()
        st.success("Importación completada.")

def page_cashflow():
    st.subheader("💵 Flujo de caja (semanal)")
    days = st.slider("Rango (días)", 14, 180, 90)
    weekends_only = st.toggle("Solo fines de semana (vie–dom)", value=True)

    # Ventas (ingresos)
    df_sales = db_query("""
        SELECT s.id, s.dish_id, d.name AS dish_name, s.qty_dishes, s.price_sale_at_time, s.created_at
        FROM sales s JOIN dishes d ON d.id=s.dish_id
        WHERE s.created_at >= datetime('now','-%d days')
        ORDER BY s.created_at DESC
    """ % days, as_df=True)
    if df_sales.empty:
        st.info("Aún no hay ventas en el rango.")
        return
    df_sales["created_at"] = pd.to_datetime(df_sales["created_at"], errors="coerce")
    df_sales["week"] = df_sales["created_at"].dt.strftime("%G-W%V")
    df_sales["dow"] = df_sales["created_at"].dt.dayofweek
    if weekends_only:
        df_sales = df_sales[df_sales["dow"].isin([4,5,6])]
    df_sales["ingreso"] = df_sales["qty_dishes"] * df_sales["price_sale_at_time"].fillna(0)

    # COGS estimado (usa receta actual y precios actuales)
    items_df = fetch_items(active_only=False)
    items_df["ppb"] = items_df.apply(price_per_base, axis=1)
    ppb = items_df.set_index("id")["ppb"].to_dict()

    cache_cost = {}
    def costo_plato_actual(dish_id:int) -> float:
        if dish_id in cache_cost: return cache_cost[dish_id]
        bom = recipe_of_dish(dish_id)
        if bom.empty:
            cache_cost[dish_id] = 0.0
        else:
            bom["costo_base"] = bom["item_id"].map(lambda iid: ppb.get(int(iid), 0.0))
            bom["consumo"] = bom.apply(lambda r: effective_consumption(r["qty_per_dish_base"], r["waste_pct"], r.get("corr_factor",1.0)), axis=1)
            cache_cost[dish_id] = float((bom["costo_base"] * bom["consumo"]).sum())
        return cache_cost[dish_id]

    df_sales["cogs_est"] = df_sales["dish_id"].map(lambda did: costo_plato_actual(int(did))) * df_sales["qty_dishes"]
    df_sales["util_bruta"] = df_sales["ingreso"] - df_sales["cogs_est"]

    # Compras (egresos) — usa costo histórico si está, si no, precio actual
    df_mov = fetch_movements(days=days)
    df_mov = df_mov[df_mov["type"]=="IN"].copy()
    if not df_mov.empty:
        join = df_mov.merge(items_df[["id","ppb"]], left_on="item_id", right_on="id", how="left")
        join["unit_cost"] = join.apply(lambda r: r["unit_cost_at_time"] if pd.notna(r["unit_cost_at_time"]) else r["ppb"], axis=1)
        join["egreso_compra"] = join["qty_base"] * join["unit_cost"].fillna(0)
        join["created_at"] = pd.to_datetime(join["created_at"], errors="coerce")
        join["week"] = join["created_at"].dt.strftime("%G-W%V")
        join["dow"] = join["created_at"].dt.dayofweek
        if weekends_only:
            join = join[join["dow"].isin([4,5,6])]
        compras_week = join.groupby("week", as_index=False)["egreso_compra"].sum()
    else:
        compras_week = pd.DataFrame(columns=["week","egreso_compra"])

    ventas_week = df_sales.groupby("week", as_index=False).agg(
        ingreso=("ingreso","sum"),
        cogs_est=("cogs_est","sum"),
        util_bruta=("util_bruta","sum")
    )
    flujo = ventas_week.merge(compras_week, on="week", how="left").fillna({"egreso_compra":0})
    flujo["neto_aprox"] = flujo["util_bruta"] - flujo["egreso_compra"]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Ingreso", f"{flujo['ingreso'].sum():,.0f} COP")
    c2.metric("COGS (est.)", f"{flujo['cogs_est'].sum():,.0f}")
    c3.metric("Util. bruta", f"{flujo['util_bruta'].sum():,.0f}")
    c4.metric("Compras", f"{flujo['egreso_compra'].sum():,.0f}")

    st.plotly_chart(px.bar(flujo.sort_values("week"), x="week", y=["ingreso","cogs_est","egreso_compra","neto_aprox"],
                           barmode="group", title="Flujo por semana"), use_container_width=True)

    st.markdown("**Platos más vendidos (cantidad)**")
    top = df_sales.groupby("dish_name", as_index=False)["qty_dishes"].sum().sort_values("qty_dishes", ascending=False).head(15)
    st.dataframe(top, use_container_width=True, hide_index=True)

# =======================
# Sidebar & Ruteo
# =======================
with st.sidebar:
    st.title("🍲 Matronas")
    page = st.radio(
        "Navegación",
        ["Dashboard","Ítems","Movimientos","Compras","Conteo físico","Platos & Recetas","Ventas","Importar","Flujo de caja"],
        index=0
    )
    st.caption("Unidades base: masa=g, volumen=ml, unidad=u  •  DB: " + DB_PATH)

if page == "Dashboard":
    page_dashboard()
elif page == "Ítems":
    page_items()
elif page == "Movimientos":
    page_movements()
elif page == "Compras":
    page_purchases()
elif page == "Conteo físico":
    page_count()
elif page == "Platos & Recetas":
    page_dishes()
elif page == "Ventas":
    page_sales()
elif page == "Importar":
    page_import()
elif page == "Flujo de caja":
    page_cashflow()
