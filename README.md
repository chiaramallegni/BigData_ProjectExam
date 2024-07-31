# BigData_ProjectExame
Il seguente documento riporta le note tecniche relative al progetto "Analisi del servizio bike-sharing nella città di Londra", svolto da Mallegni Chiara e Adilardi Matteo.

## Data
I dati relativi al progetto si dividono in due categorie:
* dataset principale: csv che riporta le corse relative al bike sharing di Londra da Novembre 2016 a Novembre 2020.
  Downloaded from: https://www.kaggle.com/datasets/ajohrn/bikeshare-usage-in-london-and-taipei-network/data
* dati di contesto: shape file che 
    Downloaded from: https://download.geofabrik.de/europe/united-kingdom/england/greater-london.html

I dati sono stati scaricati e archiviati in una share che è raggiungibile a questo link : https://1drv.ms/f/s!AisCE5v6PyBqgfsnQ_NW2Km_hHEMSA?e=WPnB43

## Library Used
Di seguito le librerie installate e richiamate.
### General and manage file
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
Il progetto è organizzato in 11 file .py.
I .py principlai sono :

    * 1- main : lancia il flusso principale di spark con eslcusione delle statistiche, di graph frames e dei grafici.
    * 2- plot_and_dash: deve essere lanciato dopo il main. Crea i plot e una dashboard.
    * 3- df_statistics: utilizza una sessione di spark per definire statistiche di vario tipo utili all'analisi. 
    * 4- londonbike_graphframe: utilizza una sessione di spark per definire analisi sul grafo delle stazioni di bike sharing di Londra. 

I seguenti contengono classi e funzioni o sono richiamati all'interno dei .py

    * set_log.py: riporta la classe che gestisce il file di log
    * variables_and_path: riporta le variabili generali utilizzate nel porgetto e i path 
    * utilities: riporta la classe con le funzioni generali utili al progetto
    * plot: riporta la classe per le funzioni per generare i grafici
    * df_cleaning_and_manage: riporta la classe che è stata utilizzata per generare, pulire e arricchire il dataset principale del Bike Sharing di Londra.
    * gdf_cleaning_and_manage: riporta la classe che è stata utilizzata per generare, pulire i dati geografici.
    * buffer_spatial_join: riporta la classe con una funzione che crea un buffer spaziale intorno ai punti xy delle stazioni e ricerca i punti di interesse 
                           nell'area di buffer.

### Ordine di esecuzione dei py:
 I 4 .py principali devono essere eseguiti in quest'ordine:
 
-1 main
-2 dashboard
-3 e 4 possono essere eseguiti in maniera indipendente a seguito del main. 


### Altre note imporatnti :

#### Avvio :
Prima di far partire ogni processo devono essere scaricati i dati dal link https://1drv.ms/f/s!AisCE5v6PyBqgfsnQ_NW2Km_hHEMSA?e=WPnB43
Quando inizia il processo per la prima volta viene richiesto di recuperare lo zip dei dati fornito, il programma si occupa in autonomia di archiviare il file dove necessario, unzipparlo e creare le cartelle necessarie al processo.


NOTA: il tempo di esecuzione non è poco....avere un po' di pazienza!!! ;)

#### Cartelle creaate dal programma sono :
    * log: che contiene i file di log generati dal porgramma.
    * exp_image : che contiene le immagini dei grafici in html. 
    * data: che contiene gli shp file e i csv


#### Visualizzazione della dashboad
Una volta eseguito il main si può far girare il py plot and dash.
Una volta eseguito compare nella console di controllo un ip. es http://127.0.0.1:8050, cliccando su questo ip si accede alla dash.




