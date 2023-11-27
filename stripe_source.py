import requests
import json
import pandas as pd

class Stripe:
    def __init__(self,token):
        self.token = token

    def extract_data(self):
        charges = []
        page_counter=0
        starting_after=''
        base_url = "https://api.stripe.com/v1/charges?limit=1"
        headers = {'Authorization': f'Basic {self.token}'}
        response = requests.get(base_url, headers=headers)
        response_json = response.json()
        response_data = response_json['data']
        charges.append(response_data)
        while(response_json['has_more']==True):
            batch_size = len(response_data)
            starting_after=response_data[batch_size - 1]["id"]
            next_page_url = f'{base_url}&starting_after={starting_after}'
            response = requests.get(next_page_url, headers=headers)
            response_json = response.json()
            response_data = response_json['data']
            charges.append(response_data)
            page_counter=page_counter+1
            print(f"This is Page{page_counter}")
        return charges





    def save_to_json_file(self,charges):
        with open("test_files/test.json", "w") as file:
            json.dump(charges, file, indent=2)

        # enter the json filename to be converted to json
        JSON_FILE = 'test_files/test.json'

        # enter the csv filename you wish to save it as
        CSV_FILE = 'csv_filename.csv'

        with open(JSON_FILE, encoding='utf-8') as f:
            df = pd.read_json(f)
        df=pd.DataFrame(charges[1])

        df.to_csv(CSV_FILE, encoding='utf-8', index=False)



        return



