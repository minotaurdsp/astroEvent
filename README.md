## astroEvent
#### 1. Kā palaist skriptu 
```
git clone https://github.com/minotaurdsp/astroEvent.git
cd astroEvent
```

#### 2. Sagatavot konfigurācijas failu pirms jebkuras darbības

```

cp config.ini.template config.ini
nano config.ini
```
```
[nasa]
api_key = key from nasa 
api_url = https://api.nasa.gov/neo/

[mysql_config]
mysql_host = 127.0.0.1
mysql_db = db name
mysql_user = db uuser
mysql_pass = db pasword

[twitter]
consumer_key = asd
consumer_secret = asd
access_token = asd
access_token_secret = asd

```

#### 3. Palaists vides stāvokļa pamata testus
```
./prepare_dev_env.sh
```

#### 4. Palaist skriptu
```
python naked.py
```

