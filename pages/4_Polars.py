import streamlit as st
import polars as pl
import sqlite3

st.set_page_config(page_title="POLARS",
                   initial_sidebar_state="collapsed",
                   )

if st.button("Ana Sayfaya Dön", use_container_width=True):
    st.switch_page("Ana_Sayfa.py")

st.title("POLARS")
st.code("""
    import polars as pl
    import sqlite3 
""")
st.write("""
    Python'da Polars, Pandas'a benzeyen ancak büyük veri kümelerinde daha iyi performans sağlamak için tasarlanmış 
    hızlı bir DataFrame kütüphanesidir. Apache Arrow ve Rust üzerine inşa edilmiştir, bu da onu analitik iş yükleri 
    için verimli hale getirir. Polars özellikle büyük veri kümelerini işlemek ve filtreleme, toplama ve dönüştürme 
    gibi işlemleri verimli bir şekilde gerçekleştirmek için kullanışlıdır.
""")


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

conn = sqlite3.connect('sqlite-sakila.db')

if qselect[:3] == "5 -":
    st.subheader("5. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak değişkene kaydetme")
    st.code("""
        film = pl.read_database("SELECT * FROM film", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
    """)
    film = pl.read_database("SELECT * FROM film", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)

    st.write("3 - Film tablosunda bulunup envanter tablosunda bulunmayan film sayısını bulma")
    st.code("""
        values_in_film = film["film_id"].unique().to_list()
        values_in_inventory = inventory["film_id"].unique().to_list()
        result = list(set(values_in_film) - set(values_in_inventory))
        not_in_inventory = len(result)
    """)
    values_in_film = film["film_id"].unique().to_list()
    values_in_inventory = inventory["film_id"].unique().to_list()
    result = list(set(values_in_film) - set(values_in_inventory))
    not_in_inventory = len(result)

    st.write("'film' tablosunda olup 'inventory' tablosunda olmayan film sayısı:", not_in_inventory)

elif qselect[:3] == "12 ":
    st.subheader("12. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        category = pl.read_database("SELECT * FROM category", conn)
        film_category = pl.read_database("SELECT * FROM film_category", conn)
        film = pl.read_database("SELECT * FROM film", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
    """)
    category = pl.read_database("SELECT * FROM category", conn)
    film_category = pl.read_database("SELECT * FROM film_category", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merge1 = category.join(film_category, how="inner", on="category_id", suffix=("_(film_category)"))
        merge2 = merge1.join(film, how="inner", on="film_id", suffix=("_(film)"))
        merge3 = merge2.join(inventory, how="inner", on="film_id", suffix=("_(inventory)"))
        merge4 = merge3.join(rental, how="inner", on="inventory_id", suffix=("_(rental)"))
    """)
    merge1 = category.join(film_category, how="inner", on="category_id", suffix=("_(film_category)"))
    merge2 = merge1.join(film, how="inner", on="film_id", suffix=("_(film)"))
    merge3 = merge2.join(inventory, how="inner", on="film_id", suffix=("_(inventory)"))
    merge4 = merge3.join(rental, how="inner", on="inventory_id", suffix=("_(rental)"))

    st.write("4 - GroupBy İşlemleri")
    st.code("""
        groupped = merge4.group_by(["name", "title"]).agg(pl.col("rental_id").count().alias("rental_count"))
    """)
    groupped = merge4.group_by(["name", "title"]).agg(pl.col("rental_id").count().alias("rental_count"))

    st.write("5 - Sonuç Tablosu")
    result = groupped.sort(by=groupped)
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "19 ":
    st.subheader("19. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        customer = pl.read_database("SELECT * FROM customer", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
        payment = pl.read_database("SELECT * FROM payment", conn)
    """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", suffix="_r")
        merged = merged.join(payment, how="inner", on="rental_id", suffix="_p")
    """)
    merged = customer.join(rental, how="inner", on="customer_id", suffix="_r")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="_p")

    st.write("4 - Filtreleme ve GroupBy İşlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        groupped = filtered.group_by(["first_name", "last_name"]).sum().sort(by="first_name")
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    groupped = filtered.group_by(["first_name", "last_name"]).sum().sort(by="first_name")

    st.dataframe(groupped, hide_index=True)

elif qselect[:3] == "26 ":
    st.subheader("26. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
            category = pl.read_database("SELECT * FROM category", conn)
            film_category = pl.read_database("SELECT * FROM film_category", conn)
            film = pl.read_database("SELECT * FROM film", conn)
            inventory = pl.read_database("SELECT * FROM inventory", conn)
            rental = pl.read_database("SELECT * FROM rental", conn)
        """)
    category = pl.read_database("SELECT * FROM category", conn)
    film_category = pl.read_database("SELECT * FROM film_category", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
            merged = category.join(film_category, how="inner", on="category_id", suffix=("_(film_category)"))
            merged = merged.join(film, how="inner", on="film_id", suffix=("_(film)"))
            merged = merged.join(inventory, how="inner", on="film_id", suffix=("_(inventory)"))
            merged = merged.join(rental, how="inner", on="inventory_id", suffix=("_(rental)"))
        """)
    merged = category.join(film_category, how="inner", on="category_id", suffix=("_(film_category)"))
    merged = merged.join(film, how="inner", on="film_id", suffix=("_(film)"))
    merged = merged.join(inventory, how="inner", on="film_id", suffix=("_(inventory)"))
    merged = merged.join(rental, how="inner", on="inventory_id", suffix=("_(rental)"))

    st.write("4 - Filtreleme ve GroupBy İşlemleri")
    st.code("""
        filtered = merged[["name", "title", "rental_id"]]
        groupped = filtered.group_by(["name", "title"]).len().sort(by="len", descending=True)
    """)
    filtered = merged[["name", "title", "rental_id"]]
    groupped = filtered.group_by(["name", "title"]).len().sort(by="len", descending=True)
    st.dataframe(groupped, hide_index=True)

elif qselect[:3] == "33 ":
    st.subheader("33. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        customer = pl.read_database("SELECT * FROM customer", conn)
        address = pl.read_database("SELECT * FROM address", conn)
        city = pl.read_database("SELECT * FROM city", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
        payment = pl.read_database("SELECT * FROM payment", conn)
    """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    address = pl.read_database("SELECT * FROM address", conn)
    city = pl.read_database("SELECT * FROM city", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = customer.join(address, how="inner", on="address_id", suffix="_adrs")
        merged = merged.join(city, how="inner", on="city_id", suffix="_cty")
        merged = merged.join(rental, how="inner", on="customer_id", suffix="_rnt")
        merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")
    """)
    merged = customer.join(address, how="inner", on="address_id", suffix="_adrs")
    merged = merged.join(city, how="inner", on="city_id", suffix="_cty")
    merged = merged.join(rental, how="inner", on="customer_id", suffix="_rnt")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")

    st.write("4 - Filtreleme ve GroupBy İşlemleri")
    st.code("""
        filtered = merged[["city", "first_name", "last_name", "amount"]]
        groupped = filtered.group_by(["city", "first_name", "last_name"]).sum()
        result = groupped.sort(by="amount", descending=True).head(5)
    """)
    filtered = merged[["city", "first_name", "last_name", "amount"]]
    groupped = filtered.group_by(["city", "first_name", "last_name"]).sum()
    result = groupped.sort(by="amount", descending=True).head(5)
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "40 ":
    st.subheader("40. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        customer = pl.read_database("SELECT * FROM customer", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        film = pl.read_database("SELECT * FROM film", conn)
        payment = pl.read_database("SELECT * FROM payment", conn)
    """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
        merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
        merged = merged.join(film, how="inner", on="film_id", suffix="_film")
        merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")
    """)
    merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
    merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
    merged = merged.join(film, how="inner", on="film_id", suffix="_film")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")

    st.write("4 - Müşteri harcamalarının toplamına göre en çok harcama yapan müşteriyi bulma")
    st.code("""
        customer_spending = merged.group_by("customer_id").agg([pl.col("amount").sum().alias("total_spent")])
        top_customer_id = customer_spending.sort(by="total_spent", descending=True)[0]["customer_id"].to_list().pop()
    """)
    customer_spending = merged.group_by("customer_id").agg([pl.col("amount").sum().alias("total_spent")])
    top_customer_id = customer_spending.sort(by="total_spent", descending=True)[0]["customer_id"].to_list().pop()

    st.write("5 - En çok harcama yapan müşteriye göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged.filter(pl.col("customer_id") == top_customer_id)
    """)
    filtered_merged = merged.filter(pl.col("customer_id") == top_customer_id)

    st.write("6 - Müşterinin ismi, soyismi, kiraladığı filmler ve toplam harcama miktarını gruplama")
    st.code("""
        result = filtered_merged.group_by(["first_name", "last_name", "title"]).agg(pl.col("amount").sum().alias("total_spent"))
        result_sorted = result.sort(["total_spent"], descending=True)
    """)
    result = filtered_merged.group_by(["first_name", "last_name", "title"]).agg(
        pl.col("amount").sum().alias("total_spent"))
    result_sorted = result.sort(["total_spent"], descending=True)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "47 ":
    st.subheader("47. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        store = pl.read_database("SELECT * FROM store", conn)
        staff = pl.read_database("SELECT * FROM staff", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        film = pl.read_database("SELECT * FROM film", conn)
    """)
    store = pl.read_database("SELECT * FROM store", conn)
    staff = pl.read_database("SELECT * FROM staff", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    film = pl.read_database("SELECT * FROM film", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", suffix="_stf")
        merged = merged.join(rental, how="inner", on="staff_id", suffix="_rnt")
        merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
        merged = merged.join(film, how="inner", on="film_id", suffix="_flm")
    """)
    merged = store.join(staff, how="inner", on="store_id", suffix="_stf")
    merged = merged.join(rental, how="inner", on="staff_id", suffix="_rnt")
    merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
    merged = merged.join(film, how="inner", on="film_id", suffix="_flm")

    st.write("4 - Her filmin kiralanma sayısına göre en çok kiralanan filmi bulma")
    st.code("""
        rental_count = merged.group_by("film_id").len()
        top_film_id = rental_count.sort("len", descending=True)[0]["film_id"].to_list().pop()
    """)
    rental_count = merged.group_by("film_id").len()
    top_film_id = rental_count.sort("len", descending=True)[0]["film_id"].to_list().pop()

    st.write("5 - En çok kiralanan filme göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged.filter(pl.col("film_id") == top_film_id)
    """)
    filtered_merged = merged.filter(pl.col("film_id") == top_film_id)

    st.write("6 - Mağaza ID'sine göre gruplama işlemi ve kiralanma sayıları")
    st.code("""
        result = filtered_merged.group_by(["store_id", "title"]).len().rename({"len": "rental_count"})
    """)
    result = filtered_merged.group_by(["store_id", "title"]).len().rename({"len": "rental_count"})
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "54 ":
    st.subheader("54. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        customer = pl.read_database("SELECT * FROM customer", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
        payment = pl.read_database("SELECT * FROM payment", conn)
    """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
        merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")
    """)
    merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("amount").sum()).sort("first_name")
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("amount").sum()).sort("first_name").rename({"amount": "total_payment"})
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "61 ":
    st.subheader("61. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
            customer = pl.read_database("SELECT * FROM customer", conn)
            rental = pl.read_database("SELECT * FROM rental", conn)
            payment = pl.read_database("SELECT * FROM payment", conn)
        """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
            merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
            merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")
        """)
    merged = customer.join(rental, how="inner", on="customer_id", suffix="_rnt")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="_pym")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("amount").mean()).rename({"amount": "avg_payment"})
        result_sorted = result.sort("avg_payment", descending=False)
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("amount").mean()).rename(
        {"amount": "avg_payment"})
    result_sorted = result.sort("avg_payment", descending=False)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "68 ":
    st.subheader("68. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        store = pl.read_database("SELECT * FROM store", conn)
        staff = pl.read_database("SELECT * FROM staff", conn)
        rental = pl.read_database("SELECT * FROM rental", conn)
    """)
    store = pl.read_database("SELECT * FROM store", conn)
    staff = pl.read_database("SELECT * FROM staff", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", suffix="_stf")
        merged = merged.join(rental, how="inner", on="staff_id", suffix="_rnt")
    """)
    merged = store.join(staff, how="inner", on="store_id", suffix="_stf")
    merged = merged.join(rental, how="inner", on="staff_id", suffix="_rnt")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "rental_id"]]
        result = filtered.group_by("store_id").agg(pl.col("rental_id").count()).rename({"rental_id": "rental_count"})
        result_sorted = result.sort("rental_count", descending=True)
    """)
    filtered = merged[["store_id", "rental_id"]]
    result = filtered.group_by("store_id").agg(pl.col("rental_id").count()).rename({"rental_id": "rental_count"})
    result_sorted = result.sort("rental_count", descending=True)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "75 ":
    st.subheader("75. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        category = pl.read_database("SELECT * FROM category", conn)
        film_category = pl.read_database("SELECT * FROM film_category", conn)
        film = pl.read_database("SELECT * FROM film", conn)
        film_actor = pl.read_database("SELECT * FROM film_actor", conn)
    """)
    category = pl.read_database("SELECT * FROM category", conn)
    film_category = pl.read_database("SELECT * FROM film_category", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    film_actor = pl.read_database("SELECT * FROM film_actor", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = category.join(film_category, how="inner", on="category_id", suffix="_fc")
        merged = merged.join(film, how="inner", on="film_id", suffix="_film")
        merged = merged.join(film_actor, how="inner", on="film_id", suffix="_fa")
    """)
    merged = category.join(film_category, how="inner", on="category_id", suffix="_fc")
    merged = merged.join(film, how="inner", on="film_id", suffix="_film")
    merged = merged.join(film_actor, how="inner", on="film_id", suffix="_fa")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["name", "actor_id"]]
        result = filtered.group_by("name").agg(pl.col("actor_id").count()).rename({"actor_id": "actor_count"})
        result_sorted = result.sort("actor_count", descending=False)
    """)
    filtered = merged[["name", "actor_id"]]
    result = filtered.group_by("name").agg(pl.col("actor_id").count()).rename({"actor_id": "actor_count"})
    result_sorted = result.sort("actor_count", descending=False)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "82 ":
    st.subheader("82. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        store = pl.read_database("SELECT * FROM store", conn)
        staff = pl.read_database("SELECT * FROM staff", conn)
        rental =pl.read_database("SELECT * FROM rental", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        film = pl.read_database("SELECT * FROM film", conn)
        film_category = pl.read_database("SELECT * FROM film_category", conn)
        category = pl.read_database("SELECT * FROM category", conn)
    """)
    store = pl.read_database("SELECT * FROM store", conn)
    staff = pl.read_database("SELECT * FROM staff", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    film_category = pl.read_database("SELECT * FROM film_category", conn)
    category = pl.read_database("SELECT * FROM category", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", suffix="_sta")
        merged = merged.join(rental, how="inner", on="staff_id", suffix="rnt")
        merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
        merged = merged.join(film, how="inner", on="film_id", suffix="_f")
        merged = merged.join(film_category, how="inner", on="film_id", suffix="_fc")
        merged = merged.join(category, how="inner", on="category_id", suffix="_c")
    """)
    merged = store.join(staff, how="inner", on="store_id", suffix="_sta")
    merged = merged.join(rental, how="inner", on="staff_id", suffix="rnt")
    merged = merged.join(inventory, how="inner", on="inventory_id", suffix="_inv")
    merged = merged.join(film, how="inner", on="film_id", suffix="_f")
    merged = merged.join(film_category, how="inner", on="film_id", suffix="_fc")
    merged = merged.join(category, how="inner", on="category_id", suffix="_c")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "rental_id"]]
        result = filtered.group_by("store_id", "name").agg(pl.col("rental_id").count()).rename({"rental_id": "rental_count"})
        result_sorted = result.sort("rental_count", descending=True)
    """)
    filtered = merged[["store_id", "name", "rental_id"]]
    result = filtered.group_by("store_id", "name").agg(pl.col("rental_id").count()).rename(
        {"rental_id": "rental_count"})
    result_sorted = result.sort("rental_count", descending=True)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "89 ":
    st.subheader("89. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        film = pl.read_database("SELECT * FROM film", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        rental =pl.read_database("SELECT * FROM rental", conn)
    """)
    film = pl.read_database("SELECT * FROM film", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = film.join(inventory, how="inner", on="film_id", suffix="inv")
        merged = merged.join(rental, how="inner", on="inventory_id", suffix="rnt")
    """)
    merged = film.join(inventory, how="inner", on="film_id", suffix="inv")
    merged = merged.join(rental, how="inner", on="inventory_id", suffix="rnt")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["title", "rental_id"]]
        result = filtered.group_by("title").agg(pl.col("rental_id").count()).rename({"rental_id": "total_rentals"})
        result_sorted = result.sort("total_rentals", descending=False).head(1)
    """)
    filtered = merged[["title", "rental_id"]]
    result = filtered.group_by("title").agg(pl.col("rental_id").count()).rename({"rental_id": "total_rentals"})
    result_sorted = result.sort("total_rentals", descending=False).head(1)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "96 ":
    st.subheader("96. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        customer = pl.read_database("SELECT * FROM customer", conn)
        rental =pl.read_database("SELECT * FROM rental", conn)
    """)
    customer = pl.read_database("SELECT * FROM customer", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", suffix="r")
    """)
    merged = customer.join(rental, how="inner", on="customer_id", suffix="r")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "inventory_id"]]
        result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("inventory_id").count()).rename({"inventory_id": "unique_rentals"})
        result_sorted = result.sort("unique_rentals", descending=True).head(1)
    """)
    filtered = merged[["first_name", "last_name", "inventory_id"]]
    result = filtered.group_by(["first_name", "last_name"]).agg(pl.col("inventory_id").count()).rename(
        {"inventory_id": "unique_rentals"})
    result_sorted = result.sort("unique_rentals", descending=True).head(1)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "100":
    st.subheader("100. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Veritabanındaki tabloları Polars DataFrame olarak bir değişkene atama")
    st.code("""
        store = pl.read_database("SELECT * FROM store", conn)
        staff = pl.read_database("SELECT * FROM staff", conn)
        rental =pl.read_database("SELECT * FROM rental", conn)
        inventory = pl.read_database("SELECT * FROM inventory", conn)
        film = pl.read_database("SELECT * FROM film", conn)
        film_category = pl.read_database("SELECT * FROM film_category", conn)
        category = pl.read_database("SELECT * FROM category", conn)
        payment =pl.read_database("SELECT * FROM payment", conn)
    """)
    store = pl.read_database("SELECT * FROM store", conn)
    staff = pl.read_database("SELECT * FROM staff", conn)
    rental = pl.read_database("SELECT * FROM rental", conn)
    inventory = pl.read_database("SELECT * FROM inventory", conn)
    film = pl.read_database("SELECT * FROM film", conn)
    film_category = pl.read_database("SELECT * FROM film_category", conn)
    category = pl.read_database("SELECT * FROM category", conn)
    payment = pl.read_database("SELECT * FROM payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", suffix="stf")
        merged = merged.join(rental, how="inner", on="staff_id", suffix="rn")
        merged = merged.join(inventory, how="inner", on="inventory_id", suffix="inv")
        merged = merged.join(film, how="inner", on="film_id", suffix="f")
        merged = merged.join(film_category, how="inner", on="film_id", suffix="fc")
        merged = merged.join(category, how="inner", on="category_id", suffix="ca")
        merged = merged.join(payment, how="inner", on="rental_id", suffix="pym")
    """)
    merged = store.join(staff, how="inner", on="store_id", suffix="stf")
    merged = merged.join(rental, how="inner", on="staff_id", suffix="rn")
    merged = merged.join(inventory, how="inner", on="inventory_id", suffix="inv")
    merged = merged.join(film, how="inner", on="film_id", suffix="f")
    merged = merged.join(film_category, how="inner", on="film_id", suffix="fc")
    merged = merged.join(category, how="inner", on="category_id", suffix="ca")
    merged = merged.join(payment, how="inner", on="rental_id", suffix="pym")

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "amount"]]
        result = filtered.group_by(["store_id", "name"]).agg(pl.col("amount").sum()).rename({"amount": "total_revenue"})
        result_sorted = result.sort("total_revenue", descending=True)
    """)
    filtered = merged[["store_id", "name", "amount"]]
    result = filtered.group_by(["store_id", "name"]).agg(pl.col("amount").sum()).rename({"amount": "total_revenue"})
    result_sorted = result.sort("total_revenue", descending=True)
    st.dataframe(result_sorted, hide_index=True)