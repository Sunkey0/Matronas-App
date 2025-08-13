# app.py — Inventario Restaurante "Matronas"
# Streamlit + SQLite, unidades con conversión, recetas y ventas.
# --------------------------------------------
# Requisitos: streamlit, pandas, plotly, openpyxl (sqlite3/zoneinfo estándar)
# --------------------------------------------

from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import io
import uuid

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
@st.cache_resource(show_spinner=False)
def get_conn():
    conn = sqlite3.connect("matronas_inventory.db", check_same_thread=False)
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
    # Items: base_type -> mass/volume/unit; purchase_unit -> kg,g,l,ml,unidad,docena,paquete
    db_query("""
    CREATE TABLE IF NOT EXISTS items(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT UNIQUE,
        name TEXT NOT NULL,
        category TEXT,
        base_type TEXT CHECK(base_type IN ('mass','volume','unit')) NOT NULL,
        purchase_unit TEXT NOT NULL,
        content_per_purchase REAL NOT NULL DEFAULT 1.0, -- en base (g/ml/unidad)
        min_stock_base REAL DEFAULT 0,                 -- en base
        price_purchase REAL DEFAULT 0,                  -- precio por unidad de compra
        active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
    );""")
    # Movements: qty_base siempre en unidades base (+ IN/ADJ suma; OUT resta)
    db_query("""
    CREATE TABLE IF NOT EXISTS movements(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        type TEXT CHECK(type IN ('IN','OUT','ADJ')) NOT NULL,
        qty_base REAL NOT NULL,
        qty_input REAL,
        unit_input TEXT,
        note TEXT,
        user TEXT,
        created_at TEXT,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
    );""")
    # Dishes (platos) y recetas (BOM)
    db_query("""
    CREATE TABLE IF NOT EXISTS dishes(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT NOT NULL,
        price_sale REAL DEFAULT 0,
        active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
    );""")
    db_query("""
    CREATE TABLE IF NOT EXISTS recipe(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dish_id INTEGER NOT NULL,
        item_id INTEGER NOT NULL,
        qty_per_dish_base REAL NOT NULL, -- en base (g/ml/unidad)
        waste_pct REAL DEFAULT 0,        -- 0.05 = 5% merma
        FOREIGN KEY(dish_id) REFERENCES dishes(id) ON DELETE CASCADE,
        FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
        UNIQUE(dish_id, item_id)
    );""")
    # Ventas (para trazabilidad)
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
    # fallback: si el usuario dio un valor, úsalo
    return float(given or 0.0)

def to_base_from_input(item_row, qty:float, mode:str) -> float:
    """
    mode: 'compra' => qty es en unidad de compra; 'base' => qty ya en base
    """
    if mode == "base":
        return float(qty)
    # compra
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
           m.type, m.qty_base, m.qty_input, m.unit_input, m.note, m.user, m.created_at
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
    q = "SELECT id, code, name, price_sale, active, created_at, updated_at FROM dishes WHERE 1=1"
    if active_only:
        q += " AND active=1"
    q += " ORDER BY name ASC"
    return db_query(q, as_df=True)

@st.cache_data(ttl=20)
def recipe_of_dish(dish_id:int):
    q = """
    SELECT r.id, r.dish_id, r.item_id, i.sku, i.name AS item_name, i.base_type,
           r.qty_per_dish_base, r.waste_pct,
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
# CRUD básicos
# =======================
def create_item(name, category, base_type, purchase_unit, content_per_purchase, min_stock_base, price_purchase, active=True, sku=None):
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

def add_movement(item_id:int, mtype:str, qty_base:float, qty_input=None, unit_input=None, note:str="", user:str=""):
    if mtype not in ("IN","OUT","ADJ"): raise ValueError("Tipo inválido")
    db_query("""INSERT INTO movements(item_id,type,qty_base,qty_input,unit_input,note,user,created_at)
                VALUES(?,?,?,?,?,?,?,?)""",
             (item_id, mtype, float(qty_base), None if qty_input is None else float(qty_input),
              unit_input, note.strip(), user.strip() or "usuario", now_str()))
    invalidate_caches()

def create_dish(code, name, price_sale, active=True):
    ts = now_str()
    db_query("""INSERT INTO dishes(code,name,price_sale,active,created_at,updated_at)
                VALUES(?,?,?,?,?,?)""",
             (code.strip() if code else None, name.strip(), float(price_sale or 0), 1 if active else 0, ts, ts))
    invalidate_caches()

def update_dish(dish_id:int, **fields):
    sets, params = [], []
    for k,v in fields.items():
        sets.append(f"{k}=?"); params.append(v)
    sets.append("updated_at=?"); params.append(now_str()); params.append(dish_id)
    db_query(f"UPDATE dishes SET {', '.join(sets)} WHERE id = ?", tuple(params))
    invalidate_caches()

def add_recipe_line(dish_id:int, item_id:int, qty_per_dish_base:float, waste_pct:float):
    db_query("""INSERT OR REPLACE INTO recipe(dish_id,item_id,qty_per_dish_base,waste_pct)
                VALUES(?,?,?,?)""",
             (dish_id, item_id, float(qty_per_dish_base), float(waste_pct or 0)))
    invalidate_caches()

def delete_recipe_line(recipe_id:int):
    db_query("DELETE FROM recipe WHERE id=?", (recipe_id,)); invalidate_caches()

def register_sale(dish_id:int, qty_dishes:float, user:str, note:str):
    # 1) Inserta venta
    dish = db_query("SELECT price_sale FROM dishes WHERE id=?", (dish_id,), as_df=True).iloc[0]
    db_query("""INSERT INTO sales(dish_id,qty_dishes,price_sale_at_time,user,note,created_at)
                VALUES(?,?,?,?,?,?)""",
             (dish_id, float(qty_dishes), float(dish["price_sale"]), user or "usuario", note.strip(), now_str()))
    # 2) Descuenta ingredientes según receta
    bom = recipe_of_dish(dish_id)
    for _, r in bom.iterrows():
        qty_needed_base = float(r["qty_per_dish_base"]) * float(qty_dishes) * (1.0 + float(r["waste_pct"] or 0))
        add_movement(int(r["item_id"]), "OUT", qty_needed_base, None, BASE_UNIT[r["base_type"]],
                     note=f"Consumo por venta de plato (x{qty_dishes})", user=user or "usuario")

# =======================
# Componentes de página
# =======================
def fmt_base(base_type:str) -> str:
    return BASE_UNIT.get(base_type, "")

def page_dashboard():
    st.subheader("📊 Dashboard Matronas")
    data = stock_all()
    if data.empty:
        st.info("Aún no hay ítems. Crea algunos en **Ítems**.")
        return
    # KPIs
    total_activos = int((data["active"]==1).sum())
    bajos = data[(data["active"]==1) & (data["stock_base"] < data["min_stock_base"])]
    # valor inventario: precio por base * stock_base
    val = 0.0
    if not data.empty:
        tmp = data.copy()
        tmp["ppb"] = tmp.apply(price_per_base, axis=1)
        val = float((tmp["ppb"] * tmp["stock_base"]).sum())
    c1,c2,c3 = st.columns(3)
    c1.metric("Ítems activos", f"{total_activos}")
    c2.metric("Valor inventario (COP)", f"{val:,.0f}")
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
        st.markdown("**Stock por categoría (en base)**")
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
        with st.form("item_form", clear_on_submit=True):
            c1,c2,c3,c4 = st.columns(4)
            name = c1.text_input("Nombre*", placeholder="Tomate Chonto")
            category = c2.selectbox("Categoría", ["(escribe otra)"] + preset_cats, index=2)
            category = None if category=="(escribe otra)" else category
            base_lbl = c3.selectbox("Tipo base*", list(base_types.keys()), index=1)
            base_type = base_types[base_lbl]
            purchase_unit = c4.selectbox("Unidad de compra*", units_by_base[base_type])
            c5,c6,c7,c8 = st.columns(4)
            given_cpp = c5.number_input(f"Contenido por unidad de compra (en {fmt_base(base_type)})",
                                        min_value=0.0, value=0.0, step=1.0,
                                        help="Ej.: paquete=500 g → escribe 500. kg→ deja 0 (se infiere 1000 g)")
            inferred = infer_content_per_purchase(base_type, purchase_unit, given_cpp)
            c6.metric("Factor inferido", f"{inferred:.2f} {fmt_base(base_type)}")
            min_stock = c7.number_input(f"Stock mínimo (en {fmt_base(base_type)})", min_value=0.0, value=0.0, step=1.0)
            price_purchase = c8.number_input("Precio por unidad de compra (COP)", min_value=0.0, value=0.0, step=100.0)
            c9,c10 = st.columns(2)
            sku_custom = c9.text_input("SKU (opcional)")
            active = c10.checkbox("Activo", value=True)
            submit = st.form_submit_button("Guardar", use_container_width=True)
            if submit:
                if not name.strip():
                    st.error("Nombre es obligatorio.")
                elif purchase_unit=="paquete" and inferred<=0:
                    st.error("Define el contenido del paquete en unidades base (>0).")
                else:
                    try:
                        sku = create_item(name=name, category=category, base_type=base_type,
                                          purchase_unit=purchase_unit, content_per_purchase=inferred,
                                          min_stock_base=min_stock, price_purchase=price_purchase,
                                          active=active, sku=sku_custom or None)
                        st.success(f"Ítem creado (SKU: {sku}).")
                    except sqlite3.IntegrityError:
                        st.error("SKU duplicado.")

        st.divider()
        st.markdown("**Editar ítem**")
        df = fetch_items(active_only=False)
        if df.empty:
            st.info("No hay ítems aún.")
        else:
            sel = st.selectbox("Selecciona", df["name"] + " — " + df["sku"])
            row = df.loc[df["name"] + " — " + df["sku"] == sel].iloc[0]
            with st.form("edit_form"):
                c1,c2,c3,c4 = st.columns(4)
                name_e = c1.text_input("Nombre*", value=row["name"])
                category_e = c2.text_input("Categoría", value=row["category"] or "")
                base_type_e = c3.selectbox("Tipo base*", ["mass","volume","unit"],
                                           index=["mass","volume","unit"].index(row["base_type"]))
                purchase_unit_e = c4.text_input("Unidad de compra*", value=row["purchase_unit"])
                c5,c6,c7,c8 = st.columns(4)
                cpp_e = c5.number_input(f"Contenido por unidad de compra ({fmt_base(base_type_e)})",
                                        min_value=0.0, value=float(row["content_per_purchase"] or 0), step=1.0)
                min_stock_e = c6.number_input(f"Mínimo ({fmt_base(base_type_e)})",
                                              min_value=0.0, value=float(row["min_stock_base"] or 0), step=1.0)
                price_e = c7.number_input("Precio unidad de compra (COP)", min_value=0.0,
                                          value=float(row["price_purchase"] or 0), step=100.0)
                sku_e = c8.text_input("SKU", value=row["sku"])
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
            # Export
            exp = joined[["sku","name","category","base_type","purchase_unit","content_per_purchase",
                          "min_stock_base","price_purchase","stock_base"]].copy()
            cA,cB = st.columns(2)
            cA.download_button("⬇️ Excel", data=io.BytesIO(pd.ExcelWriter(io.BytesIO(), engine="openpyxl")).getbuffer(),
                               disabled=True, help="Usa el botón CSV (Excel se genera abajo)")
            out = io.BytesIO()
            with pd.ExcelWriter(out, engine="openpyxl") as w:
                exp.to_excel(w, index=False, sheet_name="items")
            cB.download_button("⬇️ Exportar Excel", data=out.getvalue(),
                               file_name="matronas_items.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               use_container_width=True)
            st.download_button("⬇️ Exportar CSV", data=exp.to_csv(index=False).encode("utf-8"),
                               file_name="matronas_items.csv", mime="text/csv",
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
            if mtype=="OUT": qbase = qbase  # se aplicará signo al sumar (case)
            add_movement(int(row["id"]), mtype, qbase, qty_input=qty_in,
                         unit_input=(row["purchase_unit"] if mode=="compra" else base_u),
                         note=note, user=user)
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
    export_cols = ["sku","name","category","base_type","purchase_unit","content_per_purchase",
                   "stock_base","conteo_base","dif_base"]
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
            c1,c2,c3 = st.columns(3)
            code = c1.text_input("Código (opcional)")
            name = c2.text_input("Nombre del plato*", placeholder="Bandeja Matrona")
            price = c3.number_input("Precio venta (COP)", min_value=0.0, value=0.0, step=100.0)
            save = st.form_submit_button("Crear plato")
            if save:
                if not name.strip():
                    st.error("Nombre es obligatorio.")
                else:
                    try:
                        create_dish(code or None, name, price, active=True)
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
                price_e = c3.number_input("Precio venta (COP)", min_value=0.0, value=float(row["price_sale"] or 0), step=100.0)
                active_e = c4.checkbox("Activo", value=bool(row["active"]))
                cA,cB = st.columns(2)
                upd = cA.form_submit_button("Actualizar")
                # eliminar no se expone para evitar pérdidas accidentales
                if upd:
                    update_dish(int(row["id"]), code=(code_e.strip() or None), name=name_e.strip(),
                                price_sale=float(price_e), active=1 if active_e else 0)
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
                col1,col2,col3 = st.columns([2,1,1])
                item_sel = col1.selectbox("Ítem", items["name"] + " — " + items["sku"])
                item_row = items.loc[items["name"] + " — " + items["sku"] == item_sel].iloc[0]
                qty = col2.number_input(f"Cantidad por porción (en {fmt_base(item_row['base_type'])})", min_value=0.0, value=0.0, step=1.0)
                waste = col3.number_input("Merma %", min_value=0.0, max_value=100.0, value=0.0, step=1.0, help="Porcentaje adicional que se pierde (ej. 5 = 5%)")
                if st.button("Agregar/Actualizar ingrediente", use_container_width=True):
                    add_recipe_line(dish_id, int(item_row["id"]), qty, waste/100.0)
                    st.success("Ingrediente guardado.")
            st.divider()
            bom = recipe_of_dish(dish_id)
            if bom.empty:
                st.info("Este plato aún no tiene ingredientes.")
            else:
                bom_show = bom.copy()
                bom_show["unidad_base"] = bom_show["base_type"].map(BASE_UNIT)
                bom_show["costo_unit_base"] = bom_show.apply(lambda r: price_per_base(r), axis=1)
                bom_show["costo_por_plato"] = (bom_show["qty_per_dish_base"] * (1.0 + bom_show["waste_pct"])) * bom_show["costo_unit_base"]
                st.dataframe(bom_show[["item_name","qty_per_dish_base","unidad_base","waste_pct",
                                       "costo_unit_base","costo_por_plato"]],
                             use_container_width=True, hide_index=True)
                # eliminar líneas
                del_id = st.selectbox("Eliminar ingrediente (opcional)", [""]
                                      + [f"{r['id']} — {r['item_name']}" for _, r in bom_show.iterrows()])
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
            bom = recipe_of_dish(int(dish["id"]))
            if bom.empty:
                st.warning("Agrega ingredientes a la receta para ver el costeo.")
            else:
                bom["costo_base"] = bom.apply(lambda r: price_per_base(r), axis=1)
                bom["consumo"] = bom["qty_per_dish_base"] * (1.0 + bom["waste_pct"])
                bom["costo_ingrediente"] = bom["costo_base"] * bom["consumo"]
                costo = float(bom["costo_ingrediente"].sum())
                margin = float(dish["price_sale"] or 0) - costo
                c1,c2,c3 = st.columns(3)
                c1.metric("Costo por plato (COP)", f"{costo:,.0f}")
                c2.metric("Precio venta (COP)", f"{float(dish['price_sale'] or 0):,.0f}")
                c3.metric("Margen unitario (COP)", f"{margin:,.0f}")
                fig = px.pie(bom, names="item_name", values="costo_ingrediente", title="Composición de costo")
                st.plotly_chart(fig, use_container_width=True)

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

# =======================
# Sidebar & Ruteo
# =======================
with st.sidebar:
    st.title("🍲 Matronas")
    page = st.radio("Navegación", ["Dashboard","Ítems","Movimientos","Conteo físico","Platos & Recetas","Ventas"], index=0)
    st.caption("Unidades base: masa=g, volumen=ml, unidad=u")

if page == "Dashboard":
    page_dashboard()
elif page == "Ítems":
    page_items()
elif page == "Movimientos":
    page_movements()
elif page == "Conteo físico":
    page_count()
elif page == "Platos & Recetas":
    page_dishes()
elif page == "Ventas":
    page_sales()
