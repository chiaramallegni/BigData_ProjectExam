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
cont_end_stat = df_cleaned.select(col("end_station_id")).count()                   #fatto questa modifica
#le cordino
cont_end_stat(col("end_station_id").desc()).show()

#conteggio giorni e frequenza           #CAMBIA I IL DF
df_londonBike_cnt = df_londonBike.count()
#df_londonBike_cnt
day_week_cnt = (df_londonBike.groupBy(col('start_day_of_week')).count()).alias("day_count")                 #fatto questa modifica
#day_week_cnt.show()
df_londonBike_cnt = day_week_cnt.withColumn('frequency_day', col("count") / df_londonBike_cnt)
df_londonBike_cnt.show()

#si potrebe fare la stessa cosa con i mesi
month_cnt = (df_londonBike.groupBy(col('start_month')).count()).alias("month_count")
df_londonBike_cnt = month_cnt.withColumn('frequency_month', col("month_count") / df_londonBike_cnt)               #fatto questa modifica

#voglio creare delle fascie orarie e vedere in quale fascia oraria vi è più utilizzo de servizio  #da capire come mettere l'orario #fallocon with column
#crea una colonna fascia oraria

mattina = df_cleaned.filter((col("start_hour") > 5 & col("start_hour") <= 11))
pranzo = df_cleaned.filter((col("start_hour") > 11 & col("start_hour") <= 14))
pomeriggio = df_cleaned.filter((col("start_hour") > 14 & col("start_hour") <= 18))
cena = df_cleaned.filter((col("start_hour") > 18 & col("start_hour") <= 22))
sera = df_cleaned.filter((col("start_hour") > 22 & col("start_hour") <= 4))
#si possono fare le analisi su questi mini set di dati come la correlazione tra la stazione e l'orario o



#questo è tutto nuovo
df_londonBike_cl = df_londonBike_cl.withColumn("day_class", when(df_londonBike_cl["start_hour"] > 5 & df_londonBike_cl["start_hour"] <= 11, 'mattina'))
df_londonBike_cl.withColumn("time_of_day",
                            when((col("start_hour") > 5) & (col("start_hour") <= 11), "morning").\
                            when((col("start_hour") > 11) & (col("start_hour") <= 14), "lunch").\
                            when((col("start_hour") > 14) & (col("start_hour") <= 18), "afternoon").\
                            when((col("start_hour") > 18) & (col("start_hour") <= 22), "dinner").\
                            otherwise("night")).show()

#altro possibile metodo
def time_of_day(start_hour):
    if start_hour > 5 & start_hour <= 11:
        return "morning"
    elif start_hour > 11 & start_hour <= 14:
        return "lunch"
    elif start_hour > 14 & start_hour <= 18:
        return "afternoon"
    elif start_hour > 18 & start_hour <= 22:
        return "dinner"
    else:
        return "night"

# Register the UDF
time_of_day_udf = udf(time_of_day, StringType())
df_londonBike_cl = df_londonBike_cl.withColumn("time_of_day", time_of_day_udf(col("start_hour")))

month_6_dur = df_londonBike_cl.filter((col("start_month") == 'June')).select('start_month', 'duration').show()
month_7_dur = df_londonBike_cl.filter((col("start_month") == 'July ')).select('start_month', 'duration').show()
month_8_dur = df_londonBike_cl.filter((col("start_month") == 'August')).select('start_month', 'duration').show()
month_9_dur = df_londonBike_cl.filter((col("start_month") == 'September')).select('start_month', 'duration').show()
month_10_dur = df_londonBike_cl.filter((col("start_month") == 'October')).select('start_month', 'duration').show()
month_11_dur = df_londonBike_cl.filter((col("start_month") == 'November')).select('start_month', 'duration').show()
month_12_dur = df_londonBike_cl.filter((col("start_month") == 'December')).select('start_month', 'duration').show()

time_of_day_cnt = (df_londonBike.groupBy(col('time_of_day')).count())
