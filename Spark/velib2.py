
### Toutes les 5 secondes on affiche les stations de velib qui sont devenues vides ###


import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pprint import pprint

sc= SparkContext()
ssc=StreamingContext(sc, 1)   # On crée un streamcontext d'intervalle 1s


stream=ssc.socketTextStream("velib.behmo.com", 9999)  # On lit les données toutes les secondes pour voir les stations vides, on compte le nombre de fois qu'une station est signalée vide, si ce nombre est égale à 1, elle vide pour la première fois 
stations = stream.map(lambda station: json.loads(station))\
.map(lambda station: (station['contract_name']+ ' ' + station['name'],station['available_bikes']))\
.filter(lambda station: station[1]==0)\
.map(lambda station: (station,1))\
.reduceByKeyAndWindow(lambda s1,s2: s1+s2,None,5,5)\
.filter(lambda station: station[1]==1)\
.pprint()




ssc.checkpoint("./checkpoint")
ssc.start()
ssc.awaitTermination()