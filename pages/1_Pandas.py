import streamlit as st
import pandas as pd
import sqlite3

st.set_page_config(page_title="PANDAS",
                   initial_sidebar_state="collapsed",
                   )

if st.button("Ana Sayfaya Dön", use_container_width=True):
    st.switch_page("Ana_Sayfa.py")

st.title("Pandas")
st.code("""
import pandas as pd
import sqlite3
""")
st.write("""
    Pandas, Python için açık kaynaklı bir veri işleme ve analiz kütüphanesidir. Verileri hem güçlü hem de sezgisel 
    bir şekilde işlemek, temizlemek ve analiz etmek için tasarlanmıştır. Pandas ile çeşitli kaynaklardan veri 
    yükleyebilir, dönüştürebilir ve karmaşık işlemleri kolaylıkla gerçekleştirebilirsiniz.
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

conn = sqlite3.connect("sqlite-sakila.db")

if qselect[:3] == "5 -":
    st.subheader("5. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
            film = pd.read_sql("select * from film", conn)
            inventory = pd.read_sql("select * from inventory", conn)
        """)

    film = pd.read_sql("select * from film", conn)
    inventory = pd.read_sql("select * from inventory", conn)

    st.write("3 - Film tablosunda bulunup envanter tablosunda bulunmayan film sayısını bulma")
    st.code("""
            values_in_film = film["film_id"]
            values_in_inventory = inventory["film_id"]
            result = list(set(values_in_film) - set(values_in_inventory))
            not_in_inventory = len(result)
        """)
    values_in_film = film["film_id"]
    values_in_inventory = inventory["film_id"]
    result = list(set(values_in_film) - set(values_in_inventory))
    not_in_inventory = len(result)

    st.write("'film' tablosunda olup 'inventory' tablosunda olmayan film sayısı:", not_in_inventory)

elif qselect[:3] == "12 ":
    st.subheader("12. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
            category = pd.read_sql("SELECT * FROM category", conn)
            film_category = pd.read_sql("SELECT * FROM film_category", conn)
            film = pd.read_sql("SELECT * FROM film", conn)
            inventory = pd.read_sql("SELECT * FROM inventory", conn)
            rental = pd.read_sql("SELECT * FROM rental", conn)
        """)
    category = pd.read_sql("SELECT * FROM category", conn)
    film_category = pd.read_sql("SELECT * FROM film_category", conn)
    film = pd.read_sql("SELECT * FROM film", conn)
    inventory = pd.read_sql("SELECT * FROM inventory", conn)
    rental = pd.read_sql("SELECT * FROM rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
            merged = pd.merge(category, film_category, how="inner", on="category_id",
                         suffixes=("(category)", "(film_category)"))
            merged = pd.merge(merged, film, on="film_id", how ="inner")
            merged = pd.merge(merged, inventory, on="film_id", how="inner")
            merged = pd.merge(merged, rental, on="inventory_id", how="inner")
        """)

    merged = pd.merge(category, film_category, how="inner", on="category_id",
                      suffixes=("(category)", "(film_category)"))
    merged = pd.merge(merged, film, on="film_id", how="inner")
    merged = pd.merge(merged, inventory, on="film_id", how="inner")
    merged = pd.merge(merged, rental, on="inventory_id", how="inner")

    st.write("4 - GroupBy İşlemleri")
    st.code("""
            groupped = merged.groupby(["name", "title"], sort="ASC").count().reset_index()
        """)
    groupped = merged.groupby(["name", "title"], sort="ASC").count().reset_index()

    st.write("5 - Sonuç Tablosu")
    result = groupped[["name", "title", "rental_id"]]
    st.dataframe(result, hide_index=True)

elif qselect[:3] == "19 ":
    st.subheader("19. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
        rental = pd.read_sql("select * from rental", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(customer, rental, on="customer_id")
        merged = pd.merge(merged, payment, on="rental_id")
    """)
    merged = pd.merge(customer, rental, on="customer_id")
    merged = pd.merge(merged, payment, on="rental_id")

    st.write("4 - GroupBy İşlemleri")
    st.code("""
        groupped = merged.groupby(["first_name", "last_name"]).sum().reset_index()
        result = groupped[["first_name", "last_name", "amount"]]
    """)
    groupped = merged.groupby(["first_name", "last_name"]).sum().reset_index()
    result = groupped[["first_name", "last_name", "amount"]]
    st.dataframe(result, hide_index=True)


elif qselect[:3] == "26 ":
    st.subheader("26. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        category = pd.read_sql("select * from category", conn)
        film_category = pd.read_sql("select * from film_category", conn)
        film = pd.read_sql("select * from film", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        rental = pd.read_sql("select * from rental", conn)
    """)
    category = pd.read_sql("select * from category", conn)
    film_category = pd.read_sql("select * from film_category", conn)
    film = pd.read_sql("select * from film", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(category, film_category, on="category_id", suffixes=("_category", "_film_cat"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
        merged = pd.merge(merged, inventory, on="film_id")
        merged = pd.merge(merged, rental, on="inventory_id")
    """)
    merged = pd.merge(category, film_category, on="category_id", suffixes=("_category", "_film_cat"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
    merged = pd.merge(merged, inventory, on="film_id")
    merged = pd.merge(merged, rental, on="inventory_id")

    st.write("4 - Filtreleme ve groupby işlemleri")
    st.code("""
        filtered = merged[["name", "title", "rental_id"]]
        groupped = filtered.groupby(["name", "title"]).count().sort_values(by="rental_id", ascending=False)
    """)
    filtered = merged[["name", "title", "rental_id"]]
    groupped = filtered.groupby(["name", "title"]).count().sort_values(by="rental_id", ascending=False)
    st.dataframe(groupped)

elif qselect[:3] == "33 ":
    st.subheader("33. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
        address = pd.read_sql("select * from address", conn)
        city = pd.read_sql("select * from city", conn)
        rental = pd.read_sql("select * from rental", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    address = pd.read_sql("select * from address", conn)
    city = pd.read_sql("select * from city", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(customer, address, on="address_id", suffixes=("cst", "add"))
        merged = pd.merge(merged, city, on="city_id", suffixes=("", "_cty"))
        merged = pd.merge(merged, rental, on="customer_id", suffixes=("", "_rent"))
        merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))
    """)
    merged = pd.merge(customer, address, on="address_id", suffixes=("cst", "add"))
    merged = pd.merge(merged, city, on="city_id", suffixes=("", "_cty"))
    merged = pd.merge(merged, rental, on="customer_id", suffixes=("", "_rent"))
    merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))

    st.write("4 - Filtreleme ve GroupBy işlemleri")
    st.code("""
        filtered = merged[["city", "first_name", "last_name", "amount"]]
        groupped = filtered.groupby(["city", 'first_name', "last_name"]).sum()
        result = groupped.sort_values(by="amount", ascending=False).head(5)
    """)
    filtered = merged[["city", "first_name", "last_name", "amount"]]
    groupped = filtered.groupby(["city", 'first_name', "last_name"]).sum()
    result = groupped.sort_values(by="amount", ascending=False).head(5)

    st.dataframe(result)

elif qselect[:3] == "40 ":
    st.subheader("40. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
        rental = pd.read_sql("select * from rental", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        film = pd.read_sql("select * from film", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    film = pd.read_sql("select * from film", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
        merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
        merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))
    """)
    merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
    merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
    merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))

    st.write("4 - Müşteri harcamalarının toplamına göre en çok harcama yapan müşteriyi bulma")
    st.code("""
        customer_spending = merged.groupby('customer_id').agg(total_spent=('amount', 'sum')).reset_index()
        top_customer_id = customer_spending.loc[customer_spending['total_spent'].idxmax(), 'customer_id']
    """)
    customer_spending = merged.groupby('customer_id').agg(total_spent=('amount', 'sum')).reset_index()
    top_customer_id = customer_spending.loc[customer_spending['total_spent'].idxmax(), 'customer_id']

    st.write("5 - En çok harcama yapan müşteriye göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged['customer_id'] == top_customer_id]
    """)
    filtered_merged = merged[merged['customer_id'] == top_customer_id]

    st.write("6 - Müşterinin ismi, soyismi, kiraladığı filmler ve toplam harcama miktarını gruplama")
    st.code("""
        result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg(total_spent=('amount', 'sum')).reset_index()
        result_sorted = result.sort_values(by='total_spent', ascending=False)
    """)
    result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg(total_spent=('amount', 'sum')).reset_index()
    result_sorted = result.sort_values(by='total_spent', ascending=False)
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "47 ":
    st.subheader("47. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        store = pd.read_sql("select * from store", conn)
        staff = pd.read_sql("select * from staff", conn)
        rental = pd.read_sql("select * from rental", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        film = pd.read_sql("select * from film", conn)
    """)
    store = pd.read_sql("select * from store", conn)
    staff = pd.read_sql("select * from staff", conn)
    rental = pd.read_sql("select * from rental", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    film = pd.read_sql("select * from film", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(store, staff, on="store_id", suffixes=("store", "_staff"))
        merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rental"))
        merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
    """)
    merged = pd.merge(store, staff, on="store_id", suffixes=("store", "_staff"))
    merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rental"))
    merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))

    st.write("4 - Her filmin kiralanma sayısına göre en çok kiralanan filmi bulma")
    st.code("""
        rental_count = merged.groupby('film_id').size().reset_index(name='rental_count')
        top_film_id = rental_count.loc[rental_count['rental_count'].idxmax(), 'film_id']
    """)
    rental_count = merged.groupby('film_id').size().reset_index(name='rental_count')
    top_film_id = rental_count.loc[rental_count['rental_count'].idxmax(), 'film_id']

    st.write("5 - En çok kiralanan filme göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged['film_id'] == top_film_id]
    """)
    filtered_merged = merged[merged['film_id'] == top_film_id]

    st.write("6 - Mağaza ID'sine göre gruplama işlemi ve kiralanma sayıları")
    st.code("""
        result = filtered_merged.groupby(['store_id', 'title']).size().reset_index(name='rental_count')
        result_sorted = result.sort_values(by='rental_count', ascending=False).head()
    """)
    result = filtered_merged.groupby(['store_id', 'title']).size().reset_index(name='rental_count')
    result_sorted = result.sort_values(by='rental_count', ascending=False).head()
    st.dataframe(result_sorted, hide_index=True)

elif qselect[:3] == "54 ":
    st.subheader("54. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
        rental = pd.read_sql("select * from rental", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
        merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))
    """)
    merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
    merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        result = filtered.groupby(["first_name", "last_name"]).agg(total_payment=("amount", "sum"))
    """)

    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(total_payment=("amount", "sum"))
    st.dataframe(result)

elif qselect[:3] == "61 ":
    st.subheader("61. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
            customer = pd.read_sql("select * from customer", conn)
            rental = pd.read_sql("select * from rental", conn)
            payment = pd.read_sql("select * from payment", conn)
        """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
            merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
            merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))
        """)
    merged = pd.merge(customer, rental, on="customer_id", suffixes=("cst", "rnt"))
    merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pym"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        result = filtered.groupby(["first_name", "last_name"]).agg(avg_payment=("amount", "mean"))
        sorted_result = result.sort_values(by="avg_payment", ascending=True)
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(avg_payment=("amount", "mean"))
    sorted_result = result.sort_values(by="avg_payment", ascending=True).head(10)
    st.dataframe(sorted_result)

elif qselect[:3] == "68 ":
    st.subheader("68. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        store = pd.read_sql("select * from store", conn)
        staff = pd.read_sql("select * from staff", conn)
        rental = pd.read_sql("select * from rental", conn)
    """)
    store = pd.read_sql("select * from store", conn)
    staff = pd.read_sql("select * from staff", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(store, staff, on="store_id", suffixes=("st", "stf"))
        merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))
    """)
    merged = pd.merge(store, staff, on="store_id", suffixes=("st", "stf"))
    merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "rental_id"]]
        result = filtered.groupby(["store_id"]).agg(rental_count=("rental_id", "count"))
        sorted_result = result.sort_values(by="rental_count", ascending=False)
    """)
    filtered = merged[["store_id", "rental_id"]]
    result = filtered.groupby(["store_id"]).agg(rental_count=("rental_id", "count"))
    sorted_result = result.sort_values(by="rental_count", ascending=False)
    st.dataframe(sorted_result)

elif qselect[:3] == "75 ":
    st.subheader("75. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        category = pd.read_sql("select * from category", conn)
        film_category = pd.read_sql("select * from film_category", conn)
        film = pd.read_sql("select * from film", conn)
        film_actor = pd.read_sql("select * from film_actor", conn)
    """)
    category = pd.read_sql("select * from category", conn)
    film_category = pd.read_sql("select * from film_category", conn)
    film = pd.read_sql("select * from film", conn)
    film_actor = pd.read_sql("select * from film_actor", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(category, film_category, on="category_id", suffixes=("_ct", "_fc"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_f"))
        merged = pd.merge(merged, film_actor, on="film_id", suffixes=("", "_fa"))
    """)
    merged = pd.merge(category, film_category, on="category_id", suffixes=("_ct", "_fc"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_f"))
    merged = pd.merge(merged, film_actor, on="film_id", suffixes=("", "_fa"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["name", "actor_id"]]
        result = filtered.groupby(["name"]).agg(actor_count=("actor_id", "count")).drop_duplicates()
        sorted_result = result.sort_values(by="actor_count", ascending=True)
    """)
    filtered = merged[["name", "actor_id"]]
    result = filtered.groupby(["name"]).agg(actor_count=("actor_id", "count")).drop_duplicates()
    sorted_result = result.sort_values(by="actor_count", ascending=True)
    st.dataframe(sorted_result)

elif qselect[:3] == "82 ":
    st.subheader("82. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        store = pd.read_sql("select * from store", conn)
        staff = pd.read_sql("select * from staff", conn)
        rental = pd.read_sql("select * from rental", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        film = pd.read_sql("select * from film", conn)
        film_category = pd.read_sql("select * from film_category", conn)
        category = pd.read_sql("select * from category", conn)
    """)
    store = pd.read_sql("select * from store", conn)
    staff = pd.read_sql("select * from staff", conn)
    rental = pd.read_sql("select * from rental", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    film = pd.read_sql("select * from film", conn)
    film_category = pd.read_sql("select * from film_category", conn)
    category = pd.read_sql("select * from category", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(store, staff, on="store_id", suffixes=("_sto", "_sta"))
        merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))
        merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
        merged = pd.merge(merged, film_category, on="film_id", suffixes=("", "_fCat"))
        merged = pd.merge(merged, category, on="category_id", suffixes=("", "_cat"))
    """)
    merged = pd.merge(store, staff, on="store_id", suffixes=("_sto", "_sta"))
    merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))
    merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
    merged = pd.merge(merged, film_category, on="film_id", suffixes=("", "_fCat"))
    merged = pd.merge(merged, category, on="category_id", suffixes=("", "_cat"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "rental_id"]]
        result = filtered.groupby(["store_id", "name"]).agg(rental_count=("rental_id", "count"))
        sorted_result = result.sort_values(by="rental_count", ascending=False)
    """)
    filtered = merged[["store_id", "name", "rental_id"]]
    result = filtered.groupby(["store_id", "name"]).agg(rental_count=("rental_id", "count"))
    sorted_result = result.sort_values(by="rental_count", ascending=False)
    st.dataframe(sorted_result)

elif qselect[:3] == "89 ":
    st.subheader("89. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        film = pd.read_sql("select * from film", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        rental = pd.read_sql("select * from rental", conn)
    """)
    film = pd.read_sql("select * from film", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(film, inventory, on="film_id", suffixes=("_f", "_inv"))
        merged = pd.merge(merged, rental, on="inventory_id", suffixes=("", "_rnt"))
    """)
    merged = pd.merge(film, inventory, on="film_id", suffixes=("_f", "_inv"))
    merged = pd.merge(merged, rental, on="inventory_id", suffixes=("", "_rnt"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["title", "rental_id"]]
        result = filtered.groupby(["title"]).agg(total_rentals=("rental_id", "count"))
        sorted_result = result.sort_values(by="total_rentals", ascending=True).head(1)
    """)
    filtered = merged[["title", "rental_id"]]
    result = filtered.groupby(["title"]).agg(total_rentals=("rental_id", "count"))
    sorted_result = result.sort_values(by="total_rentals", ascending=True).head(1)
    st.dataframe(sorted_result)

elif qselect[:3] == "96 ":
    st.subheader("96. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        customer = pd.read_sql_query("SELECT * FROM customer", conn)
        rental = pd.read_sql("select * from rental", conn)
    """)
    customer = pd.read_sql_query("SELECT * FROM customer", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(customer, rental, on="customer_id", suffixes=("_cst", "_rnt"))
    """)
    merged = pd.merge(customer, rental, on="customer_id", suffixes=("_cst", "_rnt"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "inventory_id"]]
        result = filtered.groupby(["first_name", "last_name"]).agg(unique_rentals=("inventory_id", "count")).drop_duplicates()
        sorted_result = result.sort_values(by="unique_rentals", ascending=False).head(1)
    """)
    filtered = merged[["first_name", "last_name", "inventory_id"]]
    result = filtered.groupby(["first_name", "last_name"]).agg(
        unique_rentals=("inventory_id", "count")).drop_duplicates()
    sorted_result = result.sort_values(by="unique_rentals", ascending=False).head(1)
    st.dataframe(sorted_result)

elif qselect[:3] == "100":
    st.subheader("100. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code('conn = sqlite3.connect("sqlite-sakila.db")')

    st.write("2 - Tabloları Pandas DataFrame olarak değişkene kaydetme")
    st.code("""
        store = pd.read_sql("select * from store", conn)
        staff = pd.read_sql("select * from staff", conn)
        rental = pd.read_sql("select * from rental", conn)
        inventory = pd.read_sql("select * from inventory", conn)
        film = pd.read_sql("select * from film", conn)
        film_category = pd.read_sql("select * from film_category", conn)
        category = pd.read_sql("select * from category", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    store = pd.read_sql("select * from store", conn)
    staff = pd.read_sql("select * from staff", conn)
    rental = pd.read_sql("select * from rental", conn)
    inventory = pd.read_sql("select * from inventory", conn)
    film = pd.read_sql("select * from film", conn)
    film_category = pd.read_sql("select * from film_category", conn)
    category = pd.read_sql("select * from category", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Merge işlemleri")
    st.code("""
        merged = pd.merge(store, staff, on="store_id", suffixes=("_sto", "_sta"))
        merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))
        merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
        merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
        merged = pd.merge(merged, film_category, on="film_id", suffixes=("", "_fCat"))
        merged = pd.merge(merged, category, on="category_id", suffixes=("", "_cat"))
        merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pay"))
    """)
    merged = pd.merge(store, staff, on="store_id", suffixes=("_sto", "_sta"))
    merged = pd.merge(merged, rental, on="staff_id", suffixes=("", "_rnt"))
    merged = pd.merge(merged, inventory, on="inventory_id", suffixes=("", "_inv"))
    merged = pd.merge(merged, film, on="film_id", suffixes=("", "_film"))
    merged = pd.merge(merged, film_category, on="film_id", suffixes=("", "_fCat"))
    merged = pd.merge(merged, category, on="category_id", suffixes=("", "_cat"))
    merged = pd.merge(merged, payment, on="rental_id", suffixes=("", "_pay"))

    st.write("4 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "amount"]]
        result = filtered.groupby(["store_id", "name"]).agg(total_revenue=("amount", "sum"))
        sorted_result = result.sort_values(by="total_revenue", ascending=False)
    """)
    filtered = merged[["store_id", "name", "amount"]]
    result = filtered.groupby(["store_id", "name"]).agg(total_revenue=("amount", "sum"))
    sorted_result = result.sort_values(by="total_revenue", ascending=False)
    st.dataframe(sorted_result)