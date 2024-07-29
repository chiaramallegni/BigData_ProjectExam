# BigData_ProjectExame
Il seguente documento riporta le note tecniche relative al progetto BigData_ProjectExame.

## Data
I dati relativi al porgetto si dividono in due categorie:
* dataset principale: csv che riporta le corse relative al bike sharing di Londra da Novembre 2016 a Novembre 2020.
  Downloaded from: https://www.kaggle.com/datasets/ajohrn/bikeshare-usage-in-london-and-taipei-network/data
* dati di contesto: shape file che 
    Downloaded from: https://download.geofabrik.de/europe/united-kingdom/england/greater-london.html

I dati sono stati scaricati e archiviati un una share che è raggiungibile a questo link : https://1drv.ms/f/s!AisCE5v6PyBqgfsnQ_NW2Km_hHEMSA?e=WPnB43

## Library Used
Di seguito le leibrerie istallate e richiamate
### general and manage file
    * shutil
    * tkinter
    * sys
### Logging
    * loggin
    * datetime
### Data Manage
    * pyspark
    * graphframe
    * pandas
    * geopandas
### Plots and dashboard
    * plotly.express
    * dash
    * matplotlib

## Organizzazione progetto
il porgetto è organizzato in 11 file .py.
I .py principlai sono :

    * 1- main : lancia il flusso principale di spark con eslcusione delle statistiche, di graph frame e de grafici.
    * 2- plot_and_dash: deve essere lanciato dopo il main. Crea i plot e una dashboard.
    * 3- df_statistics: utilizza una sessione di spark per definire delle statistiche di vario tipo utili all'analisi. 
    * 4- londonbike_graphframe: utilizza una sessione di spark per definire delle analisi sul grafo delle staizoni di bike sharing di londra. 

I seguenti contengono classi e funzioni o sono richiamati all'interno dei .py

    * set_log.py: riporat laa classe che gestisce il file di log
    * variables_and_path: riporta le variabili generali utilizzate nel porgetto e i path 
    * utilities: riporta la classe con delle funzioni generali utili al progetto
    * plot: riporta la classe perle funzioni per generare i grafici
    * df_cleaning_and_manage: riporata la classe che è stata utilizzate per generare, pulire e arricchire il dataset principale del Bike Sharing di londra.
    * gdf_cleaning_and_manage: riporata la classe che è stata utilizzate per generare, pulire i dati geografici.
    * buffer_spatial_join: riporta una classe con un funziona che crea un buffer spaziale intorono ai punti xy delle stazioni e ricerca i punti di interesse nell'area di buffer.

### Ordine di esecuzione dei py:
 i 4 .py principlai devono essere eseguiti in quest'ordine :

1- main
2- dashboard

 3 e 4 possono essere eseguiti in maniera indipendente. 


### Altre note imporatnti :

#### Avvio :
Prima di far partire ogni processo deono essere scaricati i dati dil link https://1drv.ms/f/s!AisCE5v6PyBqgfsnQ_NW2Km_hHEMSA?e=WPnB43
Quando parte il porcesso per la prima volta viene richiesto di recuperare lo zip dei dati fornito, il programma si occupa in autonimia di archiviare il file dove necessario, unzipparlo e crere le cartelle necessarie al processo.


NOTA: il tempo di esecuzione non è poco....avre un po' di pazienza!!! ;)

#### cartelle creaate dal programma sono :
    * log: contiene i fiel di log gemerati dal porgramma.
    * exp_image : che contiene le immagini dei grafici in html. 
    * data: che contiene gli shp file e i csv


#### visualizzazione della dasboad
Una volta fatto girare il main si può far girare il py plot and dash.
Una volta eseguito compatre nella console di controllo un ip. es http://127.0.0.1:8050, cliccando su questo ip di accede alla dash.




