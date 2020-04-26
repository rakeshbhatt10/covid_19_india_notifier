from jsonschema import validate
from jsonschema.exceptions import ValidationError
from jsonschema.exceptions import SchemaError
from jsonschema import validators


request_contact = {
    "type": "object",
    "properties":  {
        "phone_number": {
            "type": "string",
            "minLength": 2
        },
        "state_code": {
            "type": "string",
            "minLength": 4
        },
        "district_code": {
            "type": "string",
            "minLength": 4
        }
    },
    "required": ["phone_number", "state_code"],
    "additionalProperties": False
}


def validate_contact_request(data):

    try:
        validate(data, request_contact)
    except ValidationError as e:
        print (e.message, e.validator, e.validator_value, e.relative_schema_path, e.absolute_path, e.cause)
        return {"success": False, "message": e.message, 'key': e.absolute_path[0]}
    except SchemaError as e:
        print (e.message)
        return {"success": False, "message": e.message}

    return {"success": True, "data": data}
