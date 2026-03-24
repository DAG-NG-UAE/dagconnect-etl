TABLES_SCHEMA = { 
    "service_request": {
        "columns": [
            ("servicerequestkey", "BIGINT PRIMARY KEY"),
            ("servicerequestnumber", "VARCHAR(100)"),
            ("requestcategory", "VARCHAR(50)"),
            ("createddate", "TIMESTAMP WITH TIME ZONE")
        ],
        "comments": {
            "table": "Logs of user inquiries and contact requests synced from AWS.",
            "requestcategory": "Identifies the source: contact_us or enquire_now."
        }
    }, 
    "seller": { 
        "columns": [
            ("sellerkey", "BIGINT PRIMARY KEY"),
            ("sellerid", "VARCHAR(100)"),
            ("sellertype", "VARCHAR(50)"),
            ("sellername", "VARCHAR(255)"),
            ("selleremail", "VARCHAR(255)"),
            ("offerings", "TEXT"), 
            ("contactnumber", "VARCHAR(255)"), 
            ("shopaddress", "TEXT"), 
            ("latitude", "DECIMAL(10,7)"), 
            ("longitude", "DECIMAL(10,7)"), 
            ("createddate", "TIMESTAMP WITH TIME ZONE"), 
            ("isactive", "BOOLEAN")  
        ],
        "comments": {
            "table": "Logs of sellers on the platform.",
            "sellerid": "Unique identifier for the seller.",
            "sellertype": "Type of seller (e.g., retailer, service station,dealer ).",
            "sellername": "Name of the seller.",
            "selleremail": "Email address of the seller.",
            "offerings": "Products or services offered by the seller.",
            "contactnumber": "Contact number of the seller.",
            "shopaddress": "Address of the seller's shop.",
            "latitude": "Latitude of the seller's location.",
            "longitude": "Longitude of the seller's location.",
            "createddate": "Date and time when the seller was created.",
            "isactive": "Whether the seller is active."
        }
    },
    "user_login_accounts": { 
        "columns": [ 
            ("userloginaccountkey", "BIGINT PRIMARY KEY"), 
            ("loginid", "VARCHAR(100) UNIQUE"), 
            ("usercategory", "VARCHAR(100)"), 
            ("accountstatus", "VARCHAR(100)"), 
            ("deviceid", "VARCHAR(100)"), 
            ("platform", "VARCHAR(50)"), 
            ("fullname", "VARCHAR(150)"),
            ("fullphonenumber", "VARCHAR(20)"),
            ("email", "VARCHAR(100)"),
            ("permanentaddress", "TEXT"),
            ("createddate", "TIMESTAMP WITH TIME ZONE"), 
            ("lastupdatedby", "VARCHAR(100)"),
            ("isactive", "BOOLEAN"), 
            ("triggercount", "SMALLINT")
        ],
        "comments": {
            "table": "Logs of users on the platform.",
            "loginid": "Unique identifier for the login account.",
            "usercategory": "Type of user (e.g., customer, seller).",
            "accountstatus": "Status of the account (e.g., active, inactive).",
            "deviceid": "Unique identifier for the device.",
            "platform": "Platform on which the account was created (e.g., web, mobile).",
            "fullname": "Full name of the user.",
            "fullphonenumber": "Full phone number of the user.",
            "email": "Email address of the user.",
            "permanentaddress": "Permanent address of the user.",
            "createddate": "Date and time when the account was created.",
            "lastupdatedby": "User who last updated the account.",
            "isactive": "Whether the account is active.",
            "triggercount": "Number of times the account was triggered."
        }
    }, 
    "customer_user_login_account_mapping": { 
        "columns": [
            ("customeruserloginaccmappingkey", "BIGINT PRIMARY KEY"), 
            ("customerid", "VARCHAR(100)"), 
            ("loginid", " VARCHAR(100) REFERENCES user_login_accounts(loginid)"), 
            ("createdby", "VARCHAR(100)"), 
            ("createddate", "TIMESTAMP WITH TIME ZONE"), 
            ("isactive", "BOOLEAN")
        ],
        "comments": {
            "table": "Customers who have verified with their NIN and BVN.",
            "customerid": "Unique identifier for the customer.",
            "loginid": "The login id is here the actual account of the person from the user_login_accounts table",
            "createdby": "User who created the customer.",
            "createddate": "Date and time when the customer was created.",
            "isactive": "Whether the customer is active."
        }
    }, 
    "purchased_items": { 
        "columns": [
            ("purchaseditemskey", "BIGINT PRIMARY KEY"), 
            ("orderid", "VARCHAR(100)"),
            ("itemcategory", "VARCHAR(50)"), 
            ("itemdescription", "TEXT"), 
            ("serialnumber", "VARCHAR(100)"),
            ("price", "NUMERIC(20,2)"), 
            ("receipt", "TEXT"), 
            ("createdby", "VARCHAR(100) REFERENCES user_login_accounts(loginid)"), 
            ("lastupdatedby", "VARCHAR(100) REFERENCES user_login_accounts(loginid)"),
            ("createddate", "TIMESTAMP WITH TIME ZONE"), 
            ("isactive", "BOOLEAN")
        ],
        "comments": {
            "table": "Logs of purchased items on the platform. Note: the users enter this themselves",
            "orderid": "Unique identifier for the order.",
            "itemcategory": "Type of item (e.g., product, service).",
            "itemdescription": "Description of the item.",
            "serialnumber": "Serial number of the item.",
            "price": "Price of the item.",
            "receipt": "Receipt of the item.",
            "createdby": "User who added the purchase history",
            "lastupdatedby": "User who last updated the order.",
            "createddate": "Date and time when the order was created.",
            "isactive": "Whether the order is active."
        }
    }, 
    "purchased_vehicle_items": { 
        "columns": [
            ("purchasedvehicleitemskey", "BIGINT PRIMARY KEY"), 
            ("orderid", "VARCHAR(100)"),
            ("itemcategory", "VARCHAR(50)"), 
            ("itemdescription", "TEXT"), 
            ("chasisnumber", "VARCHAR(100)"),
            ("enginenumber", "VARCHAR(100)"),
            ("price", "NUMERIC(20,2)"), 
            ("receipt", "TEXT"), 
            ("createdby", "VARCHAR(100) REFERENCES user_login_accounts(loginid)"), 
            ("lastupdatedby", "VARCHAR(100) REFERENCES user_login_accounts(loginid)"),
            ("createddate", "TIMESTAMP WITH TIME ZONE"), 
            ("isactive", "BOOLEAN")
        ],
        "comments": {
            "table": "Logs of purchased vehicle items on the platform.",
            "orderid": "Unique identifier for the order.",
            "itemcategory": "Type of item (e.g., product, service).",
            "itemdescription": "Description of the item.",
            "chasisnumber": "Chasis number of the vehicle.",
            "enginenumber": "Engine number of the vehicle.",
            "price": "Price of the item.",
            "receipt": "Receipt of the item.",
            "createdby": "User who added the purchase history",
            "lastupdatedby": "User who last updated the order.",
            "createddate": "Date and time when the order was created.",
            "isactive": "Whether the order is active."
        }
    }, 
    "last_etl_run_time": {
        "columns": [
            ("id", "BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY"),
            ("last_run_time", "TIMESTAMP WITH TIME ZONE DEFAULT NOW()"),
            ("status", "VARCHAR(20)"),
            ("message", "TEXT"), 
            ("createddate", "TIMESTAMP WITH TIME ZONE DEFAULT NOW()")
        ],
        "comments": {
            "table": "Tracking the status and time of ETL runs.",
            "status": "Whether the run 'passed' or 'failed'.",
            "message": "Error details if the status is 'failed'.",
            "createddate": "Date and time when the ETL run was recorded."
        }
    }
}