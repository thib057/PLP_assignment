
### Toutes les minutes afficher les stations les plus actives ###
##    durant les 5 dernières minutes ###


import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pprint import pprint

sc= SparkContext(appName="Stations Libres")
ssc=StreamingContext(sc, 1)    # On crée un streamcontext d'intervalle 1s


stream=ssc.socketTextStream("velib.behmo.com", 9999)    # On lit les données toutes les secondes, on note le nombre de bikes disponibles, on note l'évolution en valeur absolue 
stations = stream.map(lambda station: json.loads(station))\
.map(lambda station: (station['contract_name']+ ' ' + station['name'],station['available_bikes']))\
.map(lambda station: (station[0],(0,station[1])))\
.reduceByKeyAndWindow(lambda s1, s2: (s1[0]+abs(s1[1]-s2[1]),s2[1]),None,5*60,1*60)\
.transform(lambda rdd: rdd.sortBy(lambda s: -s[1][0]))\
.map(lambda station: (station[0], station[1][0]))\
.pprint()    # on affiche le resultat sous la forme (station, nombre de prets et retours)



ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()

