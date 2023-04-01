import requests
import json

url = ' '

payload = {
			"submitterEmail": " ", # your email as it appears on the MP instruction page
			"secret": " ", # your secret key as it appears on the MP instruction page
			"dbApi": " " # Your API Gateway POST API invoke url
		}

print(json.dumps(payload))
print("Running the autograder. This might take several seconds...")
r = requests.post(url, data=json.dumps(payload), headers = {"Content-Type": "application/json"})


print(r)
print(r.status_code, r.reason)
print(r.text)
