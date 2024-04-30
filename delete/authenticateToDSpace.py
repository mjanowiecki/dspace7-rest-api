import requests


def authenticate_to_dspace(base_url, user, password):
    # Get XSRF token
    s = requests.Session()
    status = s.get(base_url+'/api/authn/status')
    status_header = status.headers
    token = status_header.get('DSPACE-XSRF-TOKEN')
    s.cookies.update({'X-XSRF-Token': token})
    s.headers.update({'User-Agent': 'DSpace Python REST Client',
                      'X-XSRF-TOKEN': token})
    params = {'user': user, 'password': password}

    # Authenticate to DSpace site
    login = s.post(base_url+'/authn/login', data=params)
    print('logged in')
    login_header = login.headers
    new_token = login_header.get('DSPACE-XSRF-TOKEN')
    auth_token = login_header.get('Authorization')
    s.headers.update({'Authorization': auth_token,
                      'X-XSRF-TOKEN': new_token})
    return s
