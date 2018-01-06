
### Toutes les 5 secondes on affiche les stations de velib vides ###



import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pprint import pprint

sc= SparkContext(appName="Stations Libres")
ssc=StreamingContext(sc, 5)    # On crée un streamcontext d'intervalle 5s

stream=ssc.socketTextStream("velib.behmo.com", 9999)    # On lit les données toutes les 5 secondes puis on selectionne les stations dont 'available_bikes'=0
stations = stream.map(lambda station: json.loads(station))\
.map(lambda station: (station['contract_name']+ ' ' + station['name'],station['available_bikes']))\
.filter(lambda station: station[1]==0)\
.pprint()





ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()

