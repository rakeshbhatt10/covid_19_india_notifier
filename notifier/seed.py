import csv
from db_uitlity import get_covid_19_database
import json

def insert_state_list():

    file = open("notifier/data/states.csv", "r")
    reader = csv.DictReader(file)

    db_handler = get_covid_19_database()

    normlized_data = []

    for row in reader:
        # print(row)
        data = {
            "code": row.get("code").lower().strip(),
            "name": row.get("name").lower().strip(),
            "label": row.get("name").strip(),
            "category": row.get("category").lower().strip(),
        }

        # state_id = db_handler.insert(collection="states", data=data)
        # print ("State Id : ", state_id)

        normlized_data.append(data)

    f = open("states.json", "w")
    f.write(json.dumps(normlized_data, indent=2))
    f.close()



