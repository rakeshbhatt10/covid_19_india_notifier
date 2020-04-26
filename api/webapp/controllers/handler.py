from datetime import datetime


import sys

sys.path.append("../")

from db_uitlity import get_covid_19_database


def register_contact(contact):

    db_handler = get_covid_19_database()

    contact["timestamp"] = datetime.utcnow()

    contact_id = db_handler.upsert(collection="contacts", query={"phone_number": contact.get("phone_number")}, update_data={"$set": contact})

    return contact_id

