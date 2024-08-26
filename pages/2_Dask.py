import streamlit as st
import dask.dataframe as dd


st.set_page_config(page_title="DASK",
                   initial_sidebar_state="collapsed",
                   )

if st.button("Ana Sayfaya Dön", use_container_width=True):
    st.switch_page("Ana_Sayfa.py")

st.title("DASK")
st.code("""
import dask.dataframe as dd
""")
st.write("Dask, kullanıcıların CPU ve bellek kaynaklarını eşzamanlı ve dağıtılmış hesaplama için en iyi şekilde "
         "kullanmalarını sağlayan Python'da açık kaynaklı bir paralel hesaplama kütüphanesidir. Özellikle bellekten "
         "daha büyük veri kümelerini işlemek, hesaplamaları paralelleştirmek ve makine kümelerine ölçeklendirmek için "
         "kullanışlıdır.")


sorular = [
    "5 - Envanterde olmayan filmler var mı ve varsa kaç tane?",
    "12 - Kiralanabilir Filmler İçin Türlerine Göre Her Filmin Kaç Kez Kiralandığı",
    "19 - Her müşteri için toplam harcama miktarını bulun",
    "26 - Her türde en popüler filmi bulun",
    "33 - En çok kazanç sağlayan 5 müşteriyi hangi şehirde bulun?",
    "40 - En çok kazanç sağlayan müşteri hangi filmleri kiralamış?",
    "47 - En çok kiralanan film hangi mağazada kiralanmış?",
    "54 - Her müşteri için toplam ödeme miktarını bulun.",
    "61 - En az kazanç sağlayan müşterilerin ortalama ödeme miktarı nedir?",
    "68 - Hangi mağazalarda en çok film kiralanmış?",
    "75 - Hangi kategorilerde en az aktör oynamış?",
    "82 - Hangi mağazalarda hangi kategoriler en çok kiralanmış?",
    "89 - En az sayıda kiralanan film hangisidir ve toplam kiralama sayısı nedir?",
    "96 - En fazla sayıda farklı film kiralayan müşteri hangisidir?",
    "100 - Hangi mağazada, hangi kategoride en fazla kazanç sağlanmış?"
]

st.write("""
<style>
.stSelectbox div {
  border-color: #4CAF50;
}
</style>
""", unsafe_allow_html=True)
qselect = st.selectbox("Çıktısını görmek istediğiniz ETL örneğini seçiniz", sorular)

conn = 'sqlite:///sqlite-sakila.db'

if qselect[:3] == "5 -":
    st.subheader("5. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code('film_df = dd.read_sql_table("film", conn, index_col="film_id").reset_index()\n'
            'inventory_df = dd.read_sql_table(\'inventory\', conn, index_col="film_id").reset_index()')
    film_df = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    inventory_df = dd.read_sql_table('inventory', conn, index_col="film_id").reset_index()

    st.write("3 - Film tablosunda bulunup envanter tablosunda bulunmayan film sayısını bulma")
    st.code('values_in_film = film_df["film_id"].compute().unique().tolist()\n'
            'values_in_inv = inventory_df["film_id"].compute().unique().tolist()\n'
            'result = list(set(values_in_film) - set(values_in_inv))\n'
            'not_in_inventory = len(result)')
    values_in_film = film_df["film_id"].compute().unique().tolist()
    values_in_inv = inventory_df["film_id"].compute().unique().tolist()
    result = list(set(values_in_film) - set(values_in_inv))
    not_in_inventory = len(result)

    st.write("'film' tablosunda olup 'inventory' tablosunda olmayan film sayısı:", not_in_inventory)

elif qselect[:3] == "12 ":
    st.subheader("12. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code('''
    category_df = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    film_category_df = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    film_df = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    inventory_df = dd.read_sql_table("inventory", conn, index_col="film_id").reset_index()
    rental_df = dd.read_sql_table("rental", conn, index_col="inventory_id").reset_index()
    ''')
    category_df = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    film_category_df = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    film_df = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    inventory_df = dd.read_sql_table("inventory", conn, index_col="film_id").reset_index()
    rental_df = dd.read_sql_table('rental', conn, index_col="inventory_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code('''
    merge1 = dd.merge(category_df, film_category_df, how="inner", on="category_id")
    merge2 = dd.merge(merge1, film_df, how="inner", on="film_id")
    merge3 = dd.merge(merge2, inventory_df, how="inner", on="film_id", suffixes=("__x", "__y"))
    merge4 = dd.merge(merge3, rental_df, how="inner", on="inventory_id")
    ''')
    merge1 = dd.merge(category_df, film_category_df, how="inner", on="category_id")
    merge2 = dd.merge(merge1, film_df, how="inner", on="film_id")
    merge3 = dd.merge(merge2, inventory_df, how="inner", on="film_id", suffixes=("__x", "__y"))
    merge4 = dd.merge(merge3, rental_df, how="inner", on="inventory_id")

    st.write("4 - GroupBy İşlemleri")
    st.code("""
    groupped = merge4.compute().groupby(by=["name", "title"], sort="ASC").count()
    """)
    # DASK'ın groupby fonksiyonu düzgün çalışmıyordu pandas ile yaptım
    groupped = merge4.compute().groupby(by=["name", "title"], sort="ASC").count()

    st.write("5 - Sonuç Tablosu")
    result = groupped[["rental_id"]]
    st.dataframe(result)

elif qselect[:3] == "19 ":
    st.subheader("19. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, rental, on="customer_id", how="inner")
        merged = dd.merge(merged, payment, on="rental_id", how="inner")
    """)
    merged = dd.merge(customer, rental, on="customer_id", how="inner")
    merged = dd.merge(merged, payment, on="rental_id", how="inner")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        groupped = filtered.groupby(["first_name", "last_name"]).agg({"amount": "sum"})
        result = groupped.sort_values("first_name", ascending=True)
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    groupped = filtered.groupby(["first_name", "last_name"]).agg({"amount": "sum"})
    result = groupped.sort_values("first_name", ascending=True).compute()
    st.dataframe(result)


elif qselect[:3] == "26 ":
    st.subheader("26. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
        film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="film_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    """)
    category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="film_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(category, film_category, on="category_id", how="inner", suffixes=("_cat", "_film"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
        merged = dd.merge(merged, inventory, on="film_id", how="inner", suffixes=("", "_inventory"))
        merged = dd.merge(merged, rental, on="inventory_id", how="inner", suffixes=("", "_rental"))
    """)
    merged = dd.merge(category, film_category, on="category_id", how="inner", suffixes=("_cat", "_film"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
    merged = dd.merge(merged, inventory, on="film_id", how="inner", suffixes=("", "_inventory"))
    merged = dd.merge(merged, rental, on="inventory_id", how="inner", suffixes=("", "_rental"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["name", "title", "rental_id"]]
        groupped = filtered.groupby(["name", "title"]).agg({"rental_id": "count"})
        result = groupped.sort_values("rental_id", ascending=False)
    """)
    filtered = merged[["name", "title", "rental_id"]]
    groupped = filtered.groupby(["name", "title"]).agg({"rental_id": "count"})
    result = groupped.sort_values("rental_id", ascending=False).compute()
    st.dataframe(result)

elif qselect[:3] == "33 ":
    st.subheader("33. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="address_id").reset_index()
        address = dd.read_sql_table("address", conn, index_col="address_id").reset_index()
        city = dd.read_sql_table("city", conn, index_col="city_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="address_id").reset_index()
    address = dd.read_sql_table("address", conn, index_col="address_id").reset_index()
    city = dd.read_sql_table("city", conn, index_col="city_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, address, on="address_id", how="inner", suffixes=("_cst", "_add"))
        merged = dd.merge(merged, city, on="city_id", how="inner", suffixes=("", "_city"))
        merged = dd.merge(merged, rental, on="customer_id", how="inner", suffixes=("", "_rental"))
        merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))
    """)
    merged = dd.merge(customer, address, on="address_id", how="inner", suffixes=("_cst", "_add"))
    merged = dd.merge(merged, city, on="city_id", how="inner", suffixes=("", "_city"))
    merged = dd.merge(merged, rental, on="customer_id", how="inner", suffixes=("", "_rental"))
    merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["city", "first_name", "last_name", "amount"]]
        groupped = filtered.groupby(["city", 'first_name', "last_name"]).agg({"amount": "sum"})
        result = groupped.sort_values("amount", ascending=False).head(5)
    """)
    filtered = merged[["city", "first_name", "last_name", "amount"]]
    groupped = filtered.groupby(["city", 'first_name', "last_name"]).agg({"amount": "sum"})
    result = groupped.sort_values("amount", ascending=False).head(5)
    st.dataframe(result)

elif qselect[:3] == "40 ":
    st.subheader("40. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="customer_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="customer_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
        merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inv"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
        merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))
    """)
    merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
    merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inv"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
    merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))

    st.write("4 - Müşteri harcamalarının toplamına göre en çok harcama yapan müşteriyi bulma")
    st.code("""
        customer_spending = merged.groupby("customer_id").agg(total_spent=('amount', 'sum')).reset_index()
        top_customer_id = customer_spending.sort_values(by="total_spent", ascending=False).compute().get("customer_id").values[0]
    """)
    customer_spending = merged.groupby(["customer_id"]).agg(total_spent=("amount", "sum")).reset_index()
    top_customer_id = customer_spending.sort_values(by="total_spent", ascending=False).compute().get("customer_id").values[0]

    st.write("5 - En çok harcama yapan müşteriye göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged["customer_id"] == top_customer_id]
    """)
    filtered_merged = merged[merged["customer_id"] == top_customer_id]

    st.write("6 - Müşterinin ismi, soyismi, kiraladığı filmler ve toplam harcama miktarını gruplama")
    st.code("""
        result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg(total_spent=('amount', 'sum')).reset_index()
        result_sorted = result.sort_values(by='total_spent', ascending=False)
    """)
    result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg(
        total_spent=('amount', 'sum')).reset_index()
    result_sorted = result.sort_values(by='total_spent', ascending=False).compute()
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "47 ":
    st.subheader("47. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
        staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    """)
    store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
    staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_st", "_stf"))
        merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rent"))
        merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inv"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
    """)
    merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_st", "_stf"))
    merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rent"))
    merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inv"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))

    st.write("4 - Her filmin kiralanma sayısına göre en çok kiralanan filmi bulma")
    st.code("""
        rental_count = merged.groupby("film_id").agg(rental_count=("rental_id", "count")).reset_index()
        top_film_id = rental_count.sort_values(by="rental_count", ascending=False).compute().get("film_id").values[0]
    """)
    rental_count = merged.groupby("film_id").agg(rental_count=("rental_id", "count")).reset_index()
    top_film_id = rental_count.sort_values(by="rental_count", ascending=False).compute().get("film_id").values[0]

    st.write("5 - En çok kiralanan filme göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged["film_id"] == top_film_id]
    """)
    filtered_merged = merged[merged["film_id"] == top_film_id]

    st.write("6 - Mağaza ID'sine göre gruplama işlemi ve kiralanma sayıları")
    st.code("""
        result = filtered_merged.groupby(["store_id", "title"]).agg(rental_count=(("rental_id", "count")))
        result_sorted = result.sort_values("rental_count", ascending=False).reset_index().compute()
    """)
    result = filtered_merged.groupby(["store_id", "title"]).agg(rental_count=(("rental_id", "count")))
    result_sorted = result.sort_values("rental_count", ascending=False).reset_index().compute()
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "54 ":
    st.subheader("54. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
        merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))
    """)
    merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
    merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
result = filtered.groupby(["first_name", "last_name"]).agg(total_payment=("amount", "sum")).sort_values("first_name", ascending=True).reset_index()
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(total_payment=("amount", "sum")).sort_values(
        "first_name", ascending=True).compute().reset_index()
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "61 ":
    st.subheader("61. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
        merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))
    """)
    merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rnt"))
    merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_pym"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        result = filtered.groupby(["first_name", "last_name"]).agg(avg_payment=("amount", "mean")).sort_values("avg_payment", ascending=True).compute().reset_index()
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(avg_payment=("amount", "mean")).sort_values(
        "avg_payment", ascending=True).compute().reset_index()
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "68 ":
    st.subheader("68. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
        staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    """)
    store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
    staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
            merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_st", "_stf"))
            merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rent"))
        """)
    merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_st", "_stf"))
    merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rent"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "rental_id"]]
        result = filtered.groupby("store_id").agg(rental_count=("rental_id", "count")).sort_values("rental_count", ascending=False).compute().reset_index()
    """)
    filtered = merged[["store_id", "rental_id"]]
    result = filtered.groupby("store_id").agg(rental_count=("rental_id", "count")).sort_values("rental_count",
                                                                                               ascending=False).compute().reset_index()
    st.dataframe(result, hide_index=True)

if qselect[:3] == "75 ":
    st.subheader("75. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
        film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        film_actor = dd.read_sql_table("film_actor", conn, index_col="film_id").reset_index()
    """)
    category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    film_actor = dd.read_sql_table("film_actor", conn, index_col="film_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(category, film_category, on="category_id", how="inner", suffixes=("_cat", "_fc"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_f"))
        merged = dd.merge(merged, film_actor, on="film_id", how="inner", suffixes=("", "_fa"))
    """)
    merged = dd.merge(category, film_category, on="category_id", how="inner", suffixes=("_cat", "_fc"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_f"))
    merged = dd.merge(merged, film_actor, on="film_id", how="inner", suffixes=("", "_fa"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["name", "actor_id"]]
        result = filtered.groupby("name").agg(actor_count=("actor_id", "count")).sort_values("actor_count", ascending=True).compute().reset_index()
    """)
    filtered = merged[["name", "actor_id"]]
    result = filtered.groupby("name").agg(actor_count=("actor_id", "count")).sort_values("actor_count",
                                                                                         ascending=True).compute().reset_index()
    st.dataframe(result, hide_index=True)

if qselect[:3] == "82 ":
    st.subheader("82. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
        staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
        category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    """)
    store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
    staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_store", "_staff"))
        merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rental"))
        merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inventory"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
        merged = dd.merge(merged, film_category, on="film_id", how="inner", suffixes=("", "_film_category"))
        merged = dd.merge(merged, category, on="category_id", how="inner", suffixes=("", "_category"))
    """)
    merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_store", "_staff"))
    merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rental"))
    merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inventory"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
    merged = dd.merge(merged, film_category, on="film_id", how="inner", suffixes=("", "_film_category"))
    merged = dd.merge(merged, category, on="category_id", how="inner", suffixes=("", "_category"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "rental_id"]]
        result = filtered.groupby(["store_id", "name"]).agg(rental_count=("rental_id", "count")).sort_values("rental_count", ascending=False).compute().reset_index()
    """)
    filtered = merged[["store_id", "name", "rental_id"]]
    result = filtered.groupby(["store_id", "name"]).agg(rental_count=("rental_id", "count")).sort_values("rental_count",
                                                                                                         ascending=False).compute().reset_index()
    st.dataframe(result, hide_index=True)

if qselect[:3] == "89 ":
    st.subheader("89. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    """)
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(film, inventory, on="film_id", how="inner", suffixes=("_film", "_inventory"))
        merged = dd.merge(merged, rental, on="inventory_id", how="inner", suffixes=("", "_rental"))
    """)
    merged = dd.merge(film, inventory, on="film_id", how="inner", suffixes=("_film", "_inventory"))
    merged = dd.merge(merged, rental, on="inventory_id", how="inner", suffixes=("", "_rental"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["title", "rental_id"]]
        result = filtered.groupby(["title"]).agg(total_rentals=("rental_id", "count")).sort_values("total_rentals", ascending=True).head(1).reset_index()
    """)
    filtered = merged[["title", "rental_id"]]
    result = filtered.groupby(["title"]).agg(total_rentals=("rental_id", "count")).sort_values("total_rentals",
                                                                                               ascending=True).head(
        1).reset_index()
    st.dataframe(result, hide_index=True)

if qselect[:3] == "96 ":
    st.subheader("96. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    """)
    customer = dd.read_sql_table("customer", conn, index_col="customer_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rent"))
    """)
    merged = dd.merge(customer, rental, on="customer_id", how="inner", suffixes=("_cst", "_rent"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "inventory_id"]]
        result = filtered.groupby(["first_name", "last_name"]).agg(unique_rentals=("inventory_id", "count")).sort_values("unique_rentals", ascending=False).head(1).reset_index()
    """)
    filtered = merged[["first_name", "last_name", "inventory_id"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(unique_rentals=("inventory_id", "count")).sort_values(
        "unique_rentals", ascending=False).head(1).reset_index()
    st.dataframe(result, hide_index=True)

if qselect[:3] == "100":
    st.subheader("100. Soruda Uygulanan Adımlar")

    st.write("1 - Veritabanı bağlantısı için 'connection string' oluşturma")
    st.code("conn = 'sqlite:///sqlite-sakila.db'")

    st.write("2 - Tabloları Dask DataFrame olarak değişkene kaydetme")
    st.code("""
        store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
        staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
        rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
        inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
        film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
        film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
        category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
        payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()
    """)
    store = dd.read_sql_table("store", conn, index_col="store_id").reset_index()
    staff = dd.read_sql_table("staff", conn, index_col="staff_id").reset_index()
    rental = dd.read_sql_table("rental", conn, index_col="rental_id").reset_index()
    inventory = dd.read_sql_table("inventory", conn, index_col="inventory_id").reset_index()
    film = dd.read_sql_table("film", conn, index_col="film_id").reset_index()
    film_category = dd.read_sql_table("film_category", conn, index_col="category_id").reset_index()
    category = dd.read_sql_table("category", conn, index_col="category_id").reset_index()
    payment = dd.read_sql_table("payment", conn, index_col="rental_id").reset_index()

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_store", "_staff"))
        merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rental"))
        merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inventory"))
        merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
        merged = dd.merge(merged, film_category, on="film_id", how="inner", suffixes=("", "_film_category"))
        merged = dd.merge(merged, category, on="category_id", how="inner", suffixes=("", "_category"))
        merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_payment"))
    """)
    merged = dd.merge(store, staff, on="store_id", how="inner", suffixes=("_store", "_staff"))
    merged = dd.merge(merged, rental, on="staff_id", how="inner", suffixes=("", "_rental"))
    merged = dd.merge(merged, inventory, on="inventory_id", how="inner", suffixes=("", "_inventory"))
    merged = dd.merge(merged, film, on="film_id", how="inner", suffixes=("", "_film"))
    merged = dd.merge(merged, film_category, on="film_id", how="inner", suffixes=("", "_film_category"))
    merged = dd.merge(merged, category, on="category_id", how="inner", suffixes=("", "_category"))
    merged = dd.merge(merged, payment, on="rental_id", how="inner", suffixes=("", "_payment"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "amount"]]
        result = filtered.groupby(["store_id", "name"]).agg(total_revenue=("amount", "sum")).sort_values("total_revenue", ascending=False).compute().reset_index()
    """)
    filtered = merged[["store_id", "name", "amount"]]
    result = filtered.groupby(["store_id", "name"]).agg(total_revenue=("amount", "sum")).sort_values("total_revenue",
                                                                                                     ascending=False).compute().reset_index()
    st.dataframe(result, hide_index=True)