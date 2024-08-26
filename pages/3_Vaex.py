import streamlit as st
import pandas as pd
import vaex
import sqlite3

st.set_page_config(page_title="Vaex",
                   initial_sidebar_state="collapsed",
                   )

if st.button("Ana Sayfaya Dön", use_container_width=True):
    st.switch_page("Ana_Sayfa.py")

st.title("VAEX")
st.code("""
    import pandas as pd
    import vaex
    import sqlite3
""")
st.write("""
    Vaex, büyük veri kümeleri üzerinde hızlı ve verimli analiz yapmanızı sağlayan bir Python kütüphanesidir. 
    Geleneksel veri çerçevelerine göre çok daha düşük bellek kullanımı ile milyonlarca satır ve yüzlerce sütunu 
    işleyebilir. Vaex, veri işlemlerini diskten okuma veya bellekten yapılabilecek şekilde optimize eder, bu sayede 
    veriler üzerinde anlık analizler ve görselleştirmeler gerçekleştirmenize olanak tanır. Kütüphane, veri filtreleme, 
    gruplama, toplama işlemleri gibi çeşitli veri manipülasyonlarını hızlı bir şekilde yapabilir ve bunu büyük veri 
    kümeleri için bile yüksek performansla sağlar.
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
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        film = pd.read_sql("select * from film", conn)
        inventory = pd.read_sql("select * from inventory", conn)
    """)
    film = pd.read_sql("select * from film", conn)
    inventory = pd.read_sql("select * from inventory", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        film = vaex.from_pandas(film)
        inventory = vaex.from_pandas(inventory)
    """)
    film = vaex.from_pandas(film)
    inventory = vaex.from_pandas(inventory)

    st.write("4 - Film tablosunda bulunup envanter tablosunda bulunmayan film sayısını bulma")
    st.code("""
            set1 = set(film["film_id"].unique())
            set2 = set(inventory["film_id"].unique())
            farkli_degerler = set1.symmetric_difference(set2)
            not_in_inventory = len(farkli_degerler)
        """)

    set1 = set(film["film_id"].unique())
    set2 = set(inventory["film_id"].unique())
    farkli_degerler = set1.symmetric_difference(set2)
    not_in_inventory = len(farkli_degerler)

    st.write("'film' tablosunda olup 'inventory' tablosunda olmayan film sayısı:", not_in_inventory)

elif qselect[:3] == "19 ":
    st.subheader("19. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
        rental = pd.read_sql("select * from rental", conn)
        payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        customer = vaex.from_pandas(customer)
        rental = vaex.from_pandas(rental)
        payment = vaex.from_pandas(payment)
    """)
    customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rent", allow_duplication=True)
        merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_inv", allow_duplication=True)
    """)
    merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rent", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_inv", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "sum"}).sort(by="first_name")
    """)
    groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "sum"}).sort(by="first_name")
    st.dataframe(groupped.to_pandas_df(), hide_index=True)

elif qselect[:3] == "33 ":
    st.subheader("33. Soruda Uygulanan Adımlar")
    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
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

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        customer = vaex.from_pandas(customer)
        address = vaex.from_pandas(address)
        city = vaex.from_pandas(city)
        rental = vaex.from_pandas(rental)
        payment = vaex.from_pandas(payment)
    """)
    customer = vaex.from_pandas(customer)
    address = vaex.from_pandas(address)
    city = vaex.from_pandas(city)
    rental = vaex.from_pandas(rental)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = customer.join(address, how="inner", on="address_id", rsuffix="add", allow_duplication=True)
        merged = merged.join(city, how="inner", on="city_id", rsuffix="_cty", allow_duplication=True)
        merged = merged.join(rental, how="inner", on="customer_id", rsuffix="_rent", allow_duplication=True)
        merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)
    """)
    merged = customer.join(address, how="inner", on="address_id", rsuffix="add", allow_duplication=True)
    merged = merged.join(city, how="inner", on="city_id", rsuffix="_cty", allow_duplication=True)
    merged = merged.join(rental, how="inner", on="customer_id", rsuffix="_rent", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["city", "first_name", "last_name", "amount"]]
        groupped = merged.groupby(["city", 'first_name', "last_name"]).agg({"amount": "sum"}).sort(by="amount", ascending=False)
    """)
    filtered = merged[["city", "first_name", "last_name", "amount"]]
    groupped = merged.groupby(["city", 'first_name', "last_name"]).agg({"amount": "sum"}).sort(by="amount",
                                                                                               ascending=False)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)


elif qselect[:3] == "40 ":
    st.subheader("40. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
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

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        customer = vaex.from_pandas(customer)
        rental = vaex.from_pandas(rental)
        inventory = vaex.from_pandas(inventory)
        film = vaex.from_pandas(film)
        payment = vaex.from_pandas(payment)
    """)
    customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)
    inventory = vaex.from_pandas(inventory)
    film = vaex.from_pandas(film)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
        merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
        merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)
        merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)
    """)
    merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
    merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
    merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)

    st.write("5 - Müşteri harcamalarının toplamına göre en çok harcama yapan müşteriyi bulma")
    st.code("""
        customer_spending = merged.groupby('customer_id').agg({'amount': 'sum'})
        top_customer_id = customer_spending.sort(by="amount", ascending=False)[0][0]
    """)
    customer_spending = merged.groupby('customer_id').agg({'amount': 'sum'})
    top_customer_id = customer_spending.sort(by="amount", ascending=False)[0][0]

    st.write("6 - En çok harcama yapan müşteriye göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged['customer_id'] == top_customer_id]
    """)
    filtered_merged = merged[merged['customer_id'] == top_customer_id]

    st.write("7 - Müşterinin ismi, soyismi, kiraladığı filmler ve toplam harcama miktarını gruplama")
    st.code("""
        result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg({'amount': 'sum'})
        result_sorted = result.sort(by='amount', ascending=False)
    """)
    result = filtered_merged.groupby(['first_name', 'last_name', 'title']).agg({'amount': 'sum'})
    result_sorted = result.sort(by='amount', ascending=False)
    st.dataframe(result_sorted.to_pandas_df(), hide_index=True)


elif qselect[:3] == "47 ":
    st.subheader("47. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
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

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        store = vaex.from_pandas(store)
        staff = vaex.from_pandas(staff)
        rental = vaex.from_pandas(rental)
        inventory = vaex.from_pandas(inventory)
        film = vaex.from_pandas(film)
    """)
    store = vaex.from_pandas(store)
    staff = vaex.from_pandas(staff)
    rental = vaex.from_pandas(rental)
    inventory = vaex.from_pandas(inventory)
    film = vaex.from_pandas(film)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", rsuffix="_staff", allow_duplication=True)
        merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rental", allow_duplication=True)
        merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
        merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)
    """)
    merged = store.join(staff, how="inner", on="store_id", rsuffix="_staff", allow_duplication=True)
    merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rental", allow_duplication=True)
    merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
    merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)

    st.write("5 - Her filmin kiralanma sayısına göre en çok kiralanan filmi bulma")
    st.code("""
        rental_count = merged.groupby('film_id').agg({'rental_id': 'count'})
        top_film_id = rental_count.sort(by="rental_id", ascending=False)[0][0]   
    """)
    rental_count = merged.groupby('film_id').agg({'rental_id': 'count'})
    top_film_id = rental_count.sort(by="rental_id", ascending=False)[0][0]

    st.write("6 -  En çok kiralanan filme göre diğer tabloyu filtreleme")
    st.code("""
        filtered_merged = merged[merged['film_id'] == top_film_id]
    """)
    filtered_merged = merged[merged['film_id'] == top_film_id]

    st.write("7 - Mağaza ID'sine göre gruplama işlemi ve kiralanma sayıları")
    st.code("""
        result = filtered_merged.groupby(['store_id', 'title']).agg({'rental_id': 'count'})
        result_sorted = result.sort(by='rental_id', ascending=False)
    """)
    result = filtered_merged.groupby(['store_id', 'title']).agg({'rental_id': 'count'})
    result_sorted = result.sort(by='rental_id', ascending=False)
    st.dataframe(result_sorted.to_pandas_df(), hide_index=True)


elif qselect[:3] == "54 ":
    st.subheader("54. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        customer = pd.read_sql("select * from customer", conn)
rental = pd.read_sql("select * from rental", conn)
payment = pd.read_sql("select * from payment", conn)
    """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        customer = vaex.from_pandas(customer)
rental = vaex.from_pandas(rental)
payment = vaex.from_pandas(payment)
    """)
    customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)
    """)
    merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "amount"]]
        groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "sum"}).sort(by="first_name", ascending=True)
    """)
    filtered = merged[["first_name", "last_name", "amount"]]
    groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "sum"}).sort(by="first_name", ascending=True)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)


elif qselect[:3] == "61 ":
    st.subheader("61. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
            customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)
        """)
    customer = pd.read_sql("select * from customer", conn)
    rental = pd.read_sql("select * from rental", conn)
    payment = pd.read_sql("select * from payment", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
            customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)
    payment = vaex.from_pandas(payment)
        """)
    customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
            merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)
        """)
    merged = customer.join(rental, how="inner", on="customer_id", rsuffix="rnt", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pym", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
            filtered = merged[["first_name", "last_name", "amount"]]
groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "mean"}).sort(by="amount", ascending=True)
        """)
    filtered = merged[["first_name", "last_name", "amount"]]
    groupped = merged.groupby(["first_name", "last_name"]).agg({"amount": "mean"}).sort(by="amount", ascending=True)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)


elif qselect[:3] == "68 ":
    st.subheader("68. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        store = pd.read_sql("select * from store", conn)
staff = pd.read_sql("select * from staff", conn)
rental = pd.read_sql("select * from rental", conn)
    """)
    store = pd.read_sql("select * from store", conn)
    staff = pd.read_sql("select * from staff", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        store = vaex.from_pandas(store)
staff = vaex.from_pandas(staff)
rental = vaex.from_pandas(rental)
    """)
    store = vaex.from_pandas(store)
    staff = vaex.from_pandas(staff)
    rental = vaex.from_pandas(rental)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, how="inner", on="store_id", rsuffix="stf", allow_duplication=True)
merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rnt", allow_duplication=True)
    """)
    merged = store.join(staff, how="inner", on="store_id", rsuffix="stf", allow_duplication=True)
    merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rnt", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "rental_id"]]
groupped = merged.groupby(["store_id"]).agg({"rental_id": "count"}).sort(by="rental_id", ascending=False)
    """)
    filtered = merged[["store_id", "rental_id"]]
    groupped = merged.groupby(["store_id"]).agg({"rental_id": "count"}).sort(by="rental_id", ascending=False)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)


elif qselect[:3] == "75 ":
    st.subheader("75. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
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

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        category = vaex.from_pandas(category)
film_category = vaex.from_pandas(film_category)
film = vaex.from_pandas(film)
film_actor = vaex.from_pandas(film_actor)
    """)
    category = vaex.from_pandas(category)
    film_category = vaex.from_pandas(film_category)
    film = vaex.from_pandas(film)
    film_actor = vaex.from_pandas(film_actor)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = category.join(film_category, how="inner", on="category_id", rsuffix="_fc", allow_duplication=True)
merged = merged.join(film, how="inner", on="film_id", rsuffix="_f", allow_duplication=True)
merged = merged.join(film_actor, how="inner", on="film_id", rsuffix="_fa", allow_duplication=True)
    """)
    merged = category.join(film_category, how="inner", on="category_id", rsuffix="_fc", allow_duplication=True)
    merged = merged.join(film, how="inner", on="film_id", rsuffix="_f", allow_duplication=True)
    merged = merged.join(film_actor, how="inner", on="film_id", rsuffix="_fa", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["name", "actor_id"]]
groupped = merged.groupby(["name"]).agg({"actor_id": "count"}).sort(by="actor_id", ascending=True)
    """)
    filtered = merged[["name", "actor_id"]]
    groupped = merged.groupby(["name"]).agg({"actor_id": "count"}).sort(by="actor_id", ascending=True)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)

elif qselect[:3] == "82 ":
    st.subheader("82. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        store = pd.read_sql("SELECT * FROM store", conn, index_col="store_id").reset_index()
        staff = pd.read_sql("SELECT * FROM staff", conn, index_col="staff_id").reset_index()
        rental = pd.read_sql("SELECT * FROM rental", conn, index_col="rental_id").reset_index()
        inventory = pd.read_sql("SELECT * FROM inventory", conn, index_col="film_id").reset_index()
        film = pd.read_sql("SELECT * FROM film", conn, index_col="film_id").reset_index()
        film_category = pd.read_sql("SELECT * FROM film_category", conn, index_col="category_id").reset_index()
        category = pd.read_sql("SELECT * FROM category", conn, index_col="category_id").reset_index()
    """)
    store = pd.read_sql("SELECT * FROM store", conn, index_col="store_id").reset_index()
    staff = pd.read_sql("SELECT * FROM staff", conn, index_col="staff_id").reset_index()
    rental = pd.read_sql("SELECT * FROM rental", conn, index_col="rental_id").reset_index()
    inventory = pd.read_sql("SELECT * FROM inventory", conn, index_col="film_id").reset_index()
    film = pd.read_sql("SELECT * FROM film", conn, index_col="film_id").reset_index()
    film_category = pd.read_sql("SELECT * FROM film_category", conn, index_col="category_id").reset_index()
    category = pd.read_sql("SELECT * FROM category", conn, index_col="category_id").reset_index()

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        store = vaex.from_pandas(store)
        staff = vaex.from_pandas(staff)
        rental = vaex.from_pandas(rental)
        inventory = vaex.from_pandas(inventory)
        film = vaex.from_pandas(film)
        film_category = vaex.from_pandas(film_category)
        category = vaex.from_pandas(category)
    """)
    store = vaex.from_pandas(store)
    staff = vaex.from_pandas(staff)
    rental = vaex.from_pandas(rental)
    inventory = vaex.from_pandas(inventory)
    film = vaex.from_pandas(film)
    film_category = vaex.from_pandas(film_category)
    category = vaex.from_pandas(category)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = store.join(staff, on="store_id", how='inner', rsuffix='_staff')
        merged = merged.join(rental, on="staff_id", how='inner', rsuffix='_rental', allow_duplication=True)
        merged = merged.join(inventory, on="inventory_id", how='inner', rsuffix='_inventory')
        merged = merged.join(film, on="film_id", how='inner', rsuffix='_film')
        merged = merged.join(film_category, on="film_id", how='inner', rsuffix='_filmCat')
        merged = merged.join(category, on="category_id", how='inner', rsuffix='_category')
    """)
    merged = store.join(staff, on="store_id", how='inner', rsuffix='_staff')
    merged = merged.join(rental, on="staff_id", how='inner', rsuffix='_rental', allow_duplication=True)
    merged = merged.join(inventory, on="inventory_id", how='inner', rsuffix='_inventory')
    merged = merged.join(film, on="film_id", how='inner', rsuffix='_film')
    merged = merged.join(film_category, on="film_id", how='inner', rsuffix='_filmCat')
    merged = merged.join(category, on="category_id", how='inner', rsuffix='_category')

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["store_id", "name", "rental_id"]]
        result = filtered.groupby(["store_id", "name"]).agg({"rental_id": "count"})
        sorted_result = result.sort(by="rental_id", ascending=False)
    """)
    filtered = merged[["store_id", "name", "rental_id"]]
    result = filtered.groupby(["store_id", "name"]).agg({"rental_id": "count"})
    sorted_result = result.sort(by="rental_id", ascending=False)
    st.dataframe(sorted_result.to_pandas_df(), hide_index=True)


elif qselect[:3] == "96 ":
    st.subheader("96. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
    st.code("""
        customer = pd.read_sql_query("SELECT * FROM customer", conn)
rental = pd.read_sql("select * from rental", conn)
    """)
    customer = pd.read_sql_query("SELECT * FROM customer", conn)
    rental = pd.read_sql("select * from rental", conn)

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
        customer = vaex.from_pandas(customer)
rental = vaex.from_pandas(rental)
    """)
    customer = vaex.from_pandas(customer)
    rental = vaex.from_pandas(rental)

    st.write("4 - Merge işlemleri")
    st.code("""
        merged = customer.join(rental, how="inner", on="customer_id", rsuffix="_rnt", allow_duplication=True)
    """)
    merged = customer.join(rental, how="inner", on="customer_id", rsuffix="_rnt", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
        filtered = merged[["first_name", "last_name", "inventory_id"]]
groupped = merged.groupby(["first_name", "last_name"]).agg({"inventory_id": "count"}).sort(by="inventory_id", ascending=False).head(1)
    """)
    filtered = merged[["first_name", "last_name", "inventory_id"]]
    groupped = merged.groupby(["first_name", "last_name"]).agg({"inventory_id": "count"}).sort(by="inventory_id",
                                                                                               ascending=False).head(1)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)


elif qselect[:3] == "100":
    st.subheader("100. Soruda Uygulanan Adımlar")

    st.write("1 - 'sqlite3' ile veritabanı bağlantısı oluşturma")
    st.code("conn = sqlite3.connect('sqlite-sakila.db')")

    st.write("2 - Tabloları Pandas DataFrame olarak çekme")
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

    st.write("3 - Pandas ile çekilen tabloları Vaex DataFrame'e çevirme")
    st.code("""
            store = vaex.from_pandas(store)
staff = vaex.from_pandas(staff)
rental = vaex.from_pandas(rental)
inventory = vaex.from_pandas(inventory)
film = vaex.from_pandas(film)
film_category = vaex.from_pandas(film_category)
category = vaex.from_pandas(category)
payment = vaex.from_pandas(payment)
        """)
    store = vaex.from_pandas(store)
    staff = vaex.from_pandas(staff)
    rental = vaex.from_pandas(rental)
    inventory = vaex.from_pandas(inventory)
    film = vaex.from_pandas(film)
    film_category = vaex.from_pandas(film_category)
    category = vaex.from_pandas(category)
    payment = vaex.from_pandas(payment)

    st.write("4 - Merge işlemleri")
    st.code("""
            merged = store.join(staff, how="inner", on="store_id", rsuffix="_sta", allow_duplication=True)
merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rnt", allow_duplication=True)
merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)
merged = merged.join(film_category, how="inner", on="film_id", rsuffix="_fCat", allow_duplication=True)
merged = merged.join(category, how="inner", on="category_id", rsuffix="_cat", allow_duplication=True)
merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pay", allow_duplication=True)
        """)
    merged = store.join(staff, how="inner", on="store_id", rsuffix="_sta", allow_duplication=True)
    merged = merged.join(rental, how="inner", on="staff_id", rsuffix="_rnt", allow_duplication=True)
    merged = merged.join(inventory, how="inner", on="inventory_id", rsuffix="_inv", allow_duplication=True)
    merged = merged.join(film, how="inner", on="film_id", rsuffix="_film", allow_duplication=True)
    merged = merged.join(film_category, how="inner", on="film_id", rsuffix="_fCat", allow_duplication=True)
    merged = merged.join(category, how="inner", on="category_id", rsuffix="_cat", allow_duplication=True)
    merged = merged.join(payment, how="inner", on="rental_id", rsuffix="_pay", allow_duplication=True)

    st.write("5 - Filtreleme, GroupBy ve Aggregation işlemleri")
    st.code("""
            filtered = merged[["store_id", "name", "amount"]]
groupped = merged.groupby(["store_id", "name"]).agg({"amount": "sum"}).sort(by="amount", ascending=False)
        """)
    filtered = merged[["store_id", "name", "amount"]]
    groupped = merged.groupby(["store_id", "name"]).agg({"amount": "sum"}).sort(by="amount", ascending=False)
    st.dataframe(groupped.to_pandas_df(), hide_index=True)

