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
        charges+=response_data
        while(response_json['has_more']==True):
            batch_size = len(response_data)
            starting_after=response_data[batch_size - 1]["id"]
            next_page_url = f'{base_url}&starting_after={starting_after}'
            response = requests.get(next_page_url, headers=headers)
            response_json = response.json()
            response_data = response_json['data']
            charges+=response_data
            page_counter=page_counter+1
            print(f"This is Page{page_counter}")
        return charges

    def save_to_json_file(self,charges):
        # enter the csv filename you wish to save it as
        CSV_FILE = 'csv_filename.csv'
        df=pd.DataFrame(charges)
        print (charges)
        print(df)
        df.to_csv(CSV_FILE,index=False)

        return



