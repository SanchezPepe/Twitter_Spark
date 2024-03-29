# Realtime tweet streaming using Apache Spark
Spark Streaming using Twitter API
----------------------
Proyecto para la materia de Bases de Datos No Relacionales

## Directorio
Archivo | Función
------------ | -------------
_ReadTweets.py_ | Recibe los tweets y manda al socket el texto del tweet 'limpio'
_hashtags.py_ | Similar a ReadTweets.py pero manda el hashtag del tweet
_SparkSTR.py_, _spark_hashtags.py_ | Inicia Spark, trabaja los datos del socket y grafica

## Instrucciones
Configurar las API Keys que proporciona Twitter

De ser necesario abrir el puerto utilizando el comando en Netcat
```
.\nc.exe -l -p PUERTO
```

Para correr cualquiera de las dos diferentes consultas, basta con ejecutar los bats adjuntos:
Si se desea correr la búsqueda por palabras:
1. _tweets.bat_
2. _spark.bat_

Si se desea correr la búsqueda por tendencias:
1. _trending_topics.bat_
2. _spark_tt.bat_

## Resultados

### Ocurrencias de palabras en búsqueda
![G1](Figure_1.png)

### Ocurrencias de hastags
![G2](Figure_2.png)
