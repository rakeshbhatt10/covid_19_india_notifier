
from db_uitlity import get_covid_19_database
import requests
from datetime import datetime
import functools


def get_state_wise_data_from_covid19org(db_handler):

    covid_data_url = "https://api.covid19india.org/data.json"

    covid_state_wise_url = "https://api.covid19india.org/v2/state_district_wise.json"

    sess = requests.session()

    state_wise_data = sess.get(covid_data_url).json().get("statewise", [])

    def add_state_code(state):

        state["statecode"] = "in-" + state.get("statecode").lower()
        return state

    state_wise_data = list(map(lambda district: add_state_code(district), state_wise_data))

    district_wise = sess.get(covid_state_wise_url).json()
    district_names = []

    def merge_state_code_in_district(state):

        def add_state_code(district):

            district["statecode"] = "in-"+state.get("statecode").lower()
            district["state"] = state.get("state")
            district["code"] = district.get("district").lower().replace(' ', '-').strip()
            district_names.append({
                "statecode": "in-"+state.get("statecode").lower(),
                "name": district.get("district"),
                "code": district.get("district").lower().replace(' ', '-').strip()
            })
            return district

        return list(map(lambda district: add_state_code(district), state.get("districtData")))


    district_data = list(map(lambda state: merge_state_code_in_district(state), district_wise))
    district_data = functools.reduce(lambda all_items, items: all_items + items, district_data, [])

    states_with_change = update_state_wise_data(state_wise_data, db_handler)
    district_with_change = update_district_wise_data(district_wise_data=district_data, db_handler=db_handler)

    print ("Total districts : ", len(district_names))
    update_districts(districts=district_names, db_handler=db_handler)
    return {"change_states": states_with_change, "change_districts": district_with_change}




def update_districts(districts, db_handler):

    for district_data in districts:

        update = db_handler.upsert(collection="districts", query={
            "statecode": district_data.get("statecode"),
            "code": district_data.get("code")
        }, update_data={"$set": district_data})

        print ("Distr update : ", update)



def update_state_wise_data(state_wise_data, db_handler):

    states_with_increase = []
    for state_data in state_wise_data:

        state_count = db_handler.get_count(collection="state_wise_covid_data", query={
            "statecode": state_data.get("statecode"),
            "confirmed": state_data.get("confirmed")
        })

        state_data["timestamp"] = datetime.utcnow()

        if state_count == 0:
            states_with_increase.append(state_data)

        update = db_handler.upsert(collection="state_wise_covid_data", query={
            "statecode": state_data.get("statecode"),
        }, update_data={"$set": state_data})

        print ("State update : ", update)

    return states_with_increase

def update_district_wise_data(district_wise_data, db_handler):

    district_with_increase = []
    for district_data in district_wise_data:

        state_count = db_handler.get_count(collection="district_wise_covid_data", query={
            "statecode": district_data.get("statecode"),
            "confirmed": district_data.get("confirmed"),
            "code": district_data.get("code")
        })

        district_data["timestamp"] = datetime.utcnow()

        if state_count == 0:
            district_with_increase.append(district_data)

        update = db_handler.upsert(collection="district_wise_covid_data", query={
            "statecode": district_data.get("statecode"),
            "code": district_data.get("code")
        }, update_data={"$set": district_data})

        print ("district update : ", update)

    return district_with_increase


def send_sms(message, contacts, db_handler):
    phone_numbers = list(map(lambda contact: contact.get("phone_number"), contacts))

    api_key = "sdCox5hYivRn1UfZ0qrgutmVQON2KB6T7kpHGAJL4IwXbPazjSqwfbOyvrxX8cP3MloCdkGns9hKWHmJ"

    url = "https://www.fast2sms.com/dev/bulk"


    payload = {
        "sender_id":"FSTSMS",
        "message": message,
        "language": "english",
        "route":"p",
        "numbers": ','.join(phone_numbers)
    }

    headers = {
        'authorization': api_key,
        'content-type': "application/x-www-form-urlencoded"
    }

    response = requests.request("POST", url, data=payload, headers=headers)

    db_handler.insert(collection="covid_19_sms", data={
        "sender_id":"FSTSMS",
        "message": message,
        "language": "english",
        "route":"p",
        "numbers": ','.join(phone_numbers),
        "datetime": datetime.now(),
        "response": response.json()
    })



def start_covid_19_processor():

    print("Covid 19 checks")
    db_handler = get_covid_19_database()

    data = get_state_wise_data_from_covid19org(db_handler=db_handler)
    print("Total states to be pushed : ", len(data.get("change_states")), len(data.get("change_districts")))

    print (data.get("change_districts"))

    for district_code in data.get("change_districts"):
        district_contacts = db_handler.get_rows(collection="contacts", query={"district_code": district_code.get("code"), "state_code": district_code.get("statecode")}, fields={"phone_number": 1})

        message = " New case reports " + district_code.get("district") + "\n"

        message = message + "Total confirmed cases : ", district_code.get("confirmed") + "\n"
        message = message + "Total confirmed cases : ", district_code.get("confirmed") + "\n"
        message = message + "Total deseased cases : ", district_code.get("deceased") + "\n"
        message = message + "Total recovered cases : ", district_code.get("recovered") + "\n"
        message = message + "Total active cases : ", district_code.get("active") + "\n"

        send_sms(contacts=district_contacts, db_handler=db_handler, message=message)



    for state_code in data.get("change_states"):
        state_contacts = db_handler.get_rows(collection="contacts", query={"district_code": {"$eq": None}, "state_code": state_code.get("statecode")}, fields={"phone_number": 1})

        message = " New case reports "+state_code.get("state") + "\n"
        message =  message+ " Total confirmed cases : ", state_code.get("confirmed") + "\n"
        message = message + "Total confirmed cases : ", state_code.get("confirmed") + "\n"
        message = message + "Total deseased cases : ", state_code.get("deaths") + "\n"
        message = message + "Total recovered cases : ", state_code.get("recovered") + "\n"
        message = message + "Total active cases : ", state_code.get("active") + "\n"

        send_sms(contacts=state_contacts, db_handler=db_handler, message=message)



