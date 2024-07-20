#importo le librerie
from pyspark.sql.functions import *
import pyspark.sql.functions as sf
from src.main import df_londonBike
import matplotlib.pyplot as plt
from statistics import *


df_londonBike.printSchema()
df_londonBike.summary()

#rimozione valori nulli
df_cleaned = df_londonBike.dropna()
df_cleaned.show()

#per avere un summary
df_cleaned.summary().show()

df_cleaned.dtypes

#conteggio valori univoci
unique_count = df_cleaned.select("rental_id").distinct().count()
print(f"Numero di valori unici nella colonna 'nome_colonna': {unique_count}")
#altro metodo
df_cleaned.select(count_distinct(col("rental_id"))).show()

#prove statistiche
#calcolo della kurtosi della duration
df_cleaned.select(kurtosis(col("duration"))).show()
#calcolo della skunnes
df_cleaned.select(skewness(col("duration"))).show()
#calcolo standard error
df_cleaned.select(sf.stddev(col("duration"))).show()
#calcolo della varianza
df_cleaned.select(variance(col("duration"))).show()

#metti la mediana come variabile
mediana_duration= df_cleaned(median(col("duration")))
#ho preso le stazioni più utilizzate legate alla duration
piu_utilizzate= df_cleaned.filter((col("duration") > mediana_duration)).select(col("start_station_id"), col("end_station_id"))

#ho compreso che tutti i rental id sono univoci, c'è qualcosa che non va
count = df_cleaned.select(col("rental_id")).count()
print(f"Il numero di valori  è: {count}")

#calcolo la correlazione tra la stazione di partenza e di arrivo. Il risultato è di 0.2531953725202463
df_cleaned.corr("start_station_id","end_station_id")
#calcolo di altre correlazioni
df_cleaned.corr("start_hour", "duration")
df_cleaned.corr("start_hour", "start_station_id")
df_cleaned.corr("end_hour", "end_station_id")
df_cleaned.corr("duration", "start_month")#bisognerebbe convertirlo in numero

#vedo quelle la tratta dalle stazioni che si collegano più volte
df_cleaned.groupBy((col("start_station_id"), col("end_station_id")).sum("duration")).show()

#faccio il conteggio delle stazioni di partenza più frequenti
cont_star_stat = df_cleaned.select(col("start_station_id")).count()
#le cordino
cont_star_stat(col("start_station_id").desc()).show()

#faccio il conteggio delle stazioni di arrivo più frequenti
cont_star_stat = df_cleaned.select(col("end_station_id")).count()
#le cordino
cont_star_stat(col("end_station_id").desc()).show()

#conteggio giorni e frequenza           #CAMBIA I IL DF
df_londonBike_cnt = df_londonBike.count()
#df_londonBike_cnt
day_week_cnt = (df_londonBike.groupBy(col('start_day_of_week')).count())
#day_week_cnt.show()
df_londonBike_cnt = day_week_cnt.withColumn('frequency', col("count") / df_londonBike_cnt)
df_londonBike_cnt.show()

#si potrebe fare la stessa cosa con i mesi
month_cnt = (df_londonBike.groupBy(col('start_month')).count())
df_londonBike_cnt = month_cnt.withColumn('frequency', col("count") / df_londonBike_cnt)

#voglio creare delle fascie orarie e vedere in quale fascia oraria vi è più utilizzo de servizio  #da capire come mettere l'orario #fallocon with column
#crea una colonna fascia oraria

mattina = df_cleaned.filter((col("start_hour") > 5 & col("start_hour") <= 11))
pranzo = df_cleaned.filter((col("start_hour") > 11 & col("start_hour") <= 14))
pomeriggio = df_cleaned.filter((col("start_hour") > 14 & col("start_hour") <= 18))
cena = df_cleaned.filter((col("start_hour") > 18 & col("start_hour") <= 22))
sera = df_cleaned.filter((col("start_hour") > 22 & col("start_hour") <= 4))
#si possono fare le analisi su questi mini set di dati come la correlazione tra la stazione e l'orario o


