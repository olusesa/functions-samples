import requests
import json
def zohoPostNewLead(request):
  response = requests.post("https://accounts.zoho.com/oauth/v2/token?refresh_token=1000.bd2f23943488105f7796e7c8ceeefdd9.c56d8beaf6bee68d13d49a9db67deb12&client_id=1000.66A85XAWMM4Z4D47ZC291ZUX0Y6O2D&client_secret=4ef78f62f8d637bdd8e63169af326f21d182501c74&grant_type=refresh_token")
      # Set CORS headers for the preflight request
  if request.method == 'OPTIONS':
      # Allows GET requests from any origin with the Content-Type
      # header and caches preflight response for an 3600s
      headers = {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET',
          'Access-Control-Allow-Headers': 'Content-Type',
          'Access-Control-Max-Age': '3600'
      }

      return ('', 204, headers)
  
  #if response is not None:
  accessToken = response.json()['access_token']
  v_access = 'Zoho-oauthtoken ' + accessToken 
  headers = {
          'Authorization': v_access,
          'Content-Type': 'application/json'
          
      }
  json_data = request.get_json(silent=True)
  url = 'https://www.zohoapis.com/crm/v2/' + json_data["vmodule"]
  newdata = json.dumps(json_data["info"])
  #v_data = str(json.loads(newdata.encode('utf-8')))
  v_data = str(json.loads(newdata))
  v_data1 = v_data.encode('utf-8')
  post_response = requests.post(url,headers = headers, data = v_data1)

  cors_headers={
        'Access-Control-Allow-Origin': '*'
    }
  if post_response.status_code == 200 or post_response.status_code == 201:
    return ('Success', 200, cors_headers)
  else :
    print(post_response.status_code)
    return ('Failed!', post_response.status_code, cors_headers)

