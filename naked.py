import requests
import json
import datetime
import time
import yaml

from datetime import datetime
from configparser import ConfigParser

# Nolasa arejo konfiguracijas failu 
print('Load config')
config = ConfigParser()
config.read('config.ini')

print('Asteroid processing service')

# Initiating and reading config values
print('Loading configuration from file')

# Nolasa parametrus no ieladeta konfiguracijas faila un saglaba tos python mainigajos
nasa_api_key = config.get('nasa', 'api_key')
nasa_api_url = config.get('nasa', 'api_url')

# Izvada konsole si briza datumu 
# Getting todays date
dt = datetime.now()
request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
print("Generated today's date: " + str(request_date))

# Izvada konsole NASA API pilno pieprasijumu un veic GET pieprasijumu
print("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

# Izvada konsole pieprasijumu statusu , hederi un ieguto informaciju no NASA API
print("Response status code: " + str(r.status_code))
print("Response headers: " + str(r.headers))
print("Response content: " + str(r.text))

# Ja pieprasijums bija veiksmigs turpinam darbu 
if r.status_code == 200:

  # Converte uz json formatu
	json_data = json.loads(r.text)

  # Inicialize tuksus array
	ast_safe = []
	ast_hazardous = []

	# Parbauda vai json faila eksiste konkretais key / ja ir turpinam darbu
	if 'element_count' in json_data:
		# Iegustam vertibu par asteroidu skaitu un izvada konsole
		ast_count = int(json_data['element_count'])
		print("Asteroid count today: " + str(ast_count))
    
    # Ja ir vismaz viens asteroids turpinam darbu
		if ast_count > 0:
			# Cikls caur iegutajiem datiem kur tiek parbaudits ari vai key eksiste konreta hash key elements 
			for val in json_data['near_earth_objects'][request_date]:
				if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
					# Saglaba vertibas pagaidu mainigajos
					tmp_ast_name = val['name']
					tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
					# Parbauda vai elements eksiste / ja eksiste iegust diametru kilometros ja nav ieguts diametrs aizstaj ar -2 ja nav ieguti izmeri aizstaj ar -1  
					if 'kilometers' in val['estimated_diameter']:
						if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
							tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
							tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
						else:
							tmp_ast_diam_min = -2
							tmp_ast_diam_max = -2
					else:
						tmp_ast_diam_min = -1
						tmp_ast_diam_max = -1

					tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']
					# Iegust papild informaciju par asteroidu ja nav tad izvada konsole par statusu un inizialize noklusetas vertibas 
					if len(val['close_approach_data']) > 0:
						if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
							tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
							# Converte string formata date uz realu python date objektu kuru var izmantot jau aprekiniem
							tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
							tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

							if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
								# Iegust vertibu float un pectam konverte uz integer
								tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
							else:
								tmp_ast_speed = -1

							if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
								# Noapalosana lidz 3 vertibam aiz komata
								tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
							else:
								tmp_ast_miss_dist = -1
						else:
							tmp_ast_close_appr_ts = -1
							tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
							tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
					else:
						# Izvada konsole mesidzu un inizialize vertibas
						print("No close approach data in message")
						tmp_ast_close_appr_ts = 0
						tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
						tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
						tmp_ast_speed = -1
						tmp_ast_miss_dist = -1

					# Izvada konsole datu kopsavilkumu par konkreto asteroidu
					print("------------------------------------------------------- >>")
					print("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
					print("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
					print("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km")
					
					# Pievieno asteroidu datus array / ir divi limenji safe drosais limenis un hazard ir bistamais limenis 
					# Adding asteroid data to the corresponding array
					if tmp_ast_hazardous == True:
						ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])
					else:
						ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist])

		else:
			# Izvada pazinjojumu tad kad nav konstates neviens vera njemams asteroids pietiekos tuvu zemei 
			print("No asteroids are going to hit earth today")
	# izvada konsole cik ir biestamie un drosie asteroidi .. izmera cik elementi array un tos parvers par string lai varetu apvienot ar tekstualo dalju
	print("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))
  # Ja ir vismaz viens bistams asteroids tad sasorte tos pec laika un izvada konsole
	if len(ast_hazardous) > 0:
    # Sortesana pec laika 
		ast_hazardous.sort(key = lambda x: x[4], reverse=False)
    # Cikls caur bistamo asteroidu array un izvade konsole  
		print("Today's possible apocalypse (asteroid impact on earth) times:")
		for asteroid in ast_hazardous:
			print(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))
    # Sorte to pasu array tikai soreiz pec distanci un izvada konsole 
		ast_hazardous.sort(key = lambda x: x[8], reverse=False)
		print("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
	else:
		# Konsole pazinjojums tad kad nav neviena pietiekosi tuvu asteroida zemei
		print("No asteroids close passing earth today")

else:
	# Izvads konsole par kludu pazinojumu .. respektivi izvada tad kad nav dabuts 200, izvada http response status un body kas satur pazinojumu  
	print("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
