import requests

URL = "https://digitalcommerce-a485641.sites.us2.oraclecloud.com/content/published/api/v1/items/queries"

PARAMS = {
    'q': 'type eq "Template-type1"',
    'access-token': '06a3cdc608bb3603b6a86ebea0291e1a&1574070707587'}


class CEC:
    def processCEC(self):
        print(PARAMS)
        response = requests.get(url=URL, params=PARAMS)
        jsonData = response.json()

        itemServiceParams = {
            'access-token': '06a3cdc608bb3603b6a86ebea0291e1a&1574070707587'}
        
        for item in jsonData['items']:
            itemServiceURL = item['link']['href']
            itemServiceResponse = requests.get(url=itemServiceURL, params=itemServiceParams)
            itemServiceResponseJson = itemServiceResponse.json()
            if 'template-type1_listing_page_description' in itemServiceResponseJson['data'] and len(itemServiceResponseJson['data']['template-type1_listing_page_description']) > 0: 
                print('Content fine')
            else:
                print(itemServiceResponseJson['id'])

cec = CEC()
cec.processCEC()