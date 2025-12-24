import os
import pandas as pd
import sqlite3
#region VERİTABANI OLUŞTURMA VE VERİ AKTARMA
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, 'worldcities.csv')
df = pd.read_csv(#dataframe'i oku
    file_path
)
#sayılsal kolonları dönüştür
df["population"] =pd.to_numeric(df["population"], errors='coerce')
#sqlite veritabanına bağlan
connection = sqlite3.connect("worldcities.db")
#dataframii tabloya aktar
df.to_sql("cities", connection, if_exists="replace", index=False)
#endregion

#region VERİTABANINDAN SORGULAMA YAPMA
queries = {
    "en_kalabalik_10": """
        SELECT city, country, population 
        FROM cities 
        ORDER BY population DESC 
        LIMIT 10;
    """,
    "turkiye_sehirleri": """
        SELECT city, population 
        FROM cities 
        WHERE country = 'Turkey' 
        ORDER BY population DESC
        limit 10;
    """,
    "ulke_bazli_nufus":
    """
        SELECT country, SUM(population) as total_pop
        FROM cities 
        GROUP BY country 
        ORDER BY total_pop DESC 
        LIMIT 5;
    """
}
for item in queries:#her sorguyu çalıştır ve sonucu yazdır
    print(f"\nSorgu: {item}")
    print(pd.read_sql_query(queries[item], connection))
print("Sorgular tamamlandı.")

selected_query = "en_kalabalik_10"#örnek olarak bir sorgu seç
print(pd.read_sql_query(queries[selected_query],connection))
#endregion

#bağlantıyı kapat
connection.close()