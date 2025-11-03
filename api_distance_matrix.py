api_key = "AIzaSyCIsaz4T1hoPtDAJB7Q6t2ArfgY-wZj2yg"
date = "2024-06-01"
hour = "08:00:00"
time = f"{date}T{hour}+02:00"
origin = "Place de la Concorde, Paris"
destination = "Arc de Triomphe, Paris"

request = f"""https://maps.googleapis.com/maps/api/distancematrix/json
?origins={origin}
&destinations={destination}
&units=metric
&key={api_key}
&mode=driving
"""

import requests

response = requests.get(request.replace("\n", ""))
result = response.json()
print(result)
