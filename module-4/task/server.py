import os

import pandas as pd
from pypika import Table, Query
from flask import Flask, request, make_response, jsonify
from sqlalchemy import create_engine


app = Flask(__name__)

conn_format = "postgresql://{user}:{pass}@{host}:{port}/{db}"
conn_string = conn_format.format(**{
    "user": os.environ.get("POSTGRES_USER"),
    "pass": os.environ.get("POSTGRES_PASS"),
    "host": os.environ.get("POSTGRES_HOST"),
    "port": os.environ.get("POSTGRES_PORT"),
    "db": os.environ.get("POSTGRES_DB"),
})

conn = create_engine(conn_string)


#####################
## How routing works
#####################

@app.route("/")
def request_hello():
    return "Hello from root page"


@app.route("/request_get", methods=["GET"])
def request_from_get():
    return "Request GET"


@app.route("/request_post", methods=["POST"])
def request_from_post():
    return "Request POST"


@app.route("/request_all", methods=["GET", "POST", "PUT"])
def request_all():
    template = "Request All with method {}"
    return template.format(request.method)    



###########################
## How capturing parameter
###########################

@app.route('/request_param/<value_1>/<value_2>')
def request_capture_param(value_1, value_2):
    return f'Your capture parameter {value_1} & {value_2}'


@app.route('/request_param/get', methods=["GET"])
def request_capture_param_get():
    param1 = request.args.get("param1")
    param2 = request.args.get("param2")
    return f"{param1} & {param2}"


@app.route('/request_param/post', methods=["POST"])
def request_capture_param_post():
    param1 = request.form.get("param1")
    param2 = request.form.get("param2")
    return f"{param1} & {param2}"


@app.route('/request_param/post_json', methods=["POST"])
def request_capture_param_post_json():
    json_raw = request.get_json()
    param1 = request.json.get("param1")
    param2 = request.json.get("param2")
    return f"""
    RAW: {json_raw}
    Params: {param1} & {param2}
    """


################################
## Serving data start over here
################################

@app.route("/marketing/users", methods=["GET"])
def request_users():
    query = "SELECT * FROM marketing_user"
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}


@app.route("/marketing/users/list_educations", methods=["GET"])
def request_user_educations():
    query = "SELECT DISTINCT education FROM marketing_user"
    dataframe = pd.read_sql(query, conn)
    result = dataframe['education']
    result = list(result)
    return {"data": result}


@app.route("/marketing/users/list_maritals", methods=["GET"])
def request_user_maritals():
    query = "SELECT DISTINCT marital_status FROM marketing_user"
    dataframe = pd.read_sql(query, conn)
    result = dataframe['marital_status']
    result = list(result)
    return {"data": result}


@app.route("/marketing/users/filter", methods=["GET"])
def request_users_filter():
    param_education = request.args.get("education")
    param_marital = request.args.get("maritals")

    table = Table("marketing_user")
    query = Query.from_(table).select("*")

    if param_education:
        query = query.where(table.education == param_education)
    if param_marital:
        query = query.where(table.marital_status == param_marital)

    dataframe = pd.read_sql(str(query), conn)
    result = dataframe.to_dict(orient="records")

    return {"data": result}


################################
## Technical Task
################################

@app.route('/sales/list/<type>')
def request_sales_list(type):
    query = f"SELECT DISTINCT {type} FROM supermarket_sales"
    dataframe = pd.read_sql(query, conn)
    result = dataframe[f'{type}']
    result = list(result)
    return {"data": result}


@app.route('/sales/date/<month>/<day>/<year>')
def request_sales_date(month, day, year):
    query = f"SELECT * FROM supermarket_sales WHERE Date='{month}/{day}/{year}'"
    dataframe = pd.read_sql(query, conn)
    result = dataframe['invoice_id']
    result = list(result)
    return {"data": result}


@app.route('/sales/summary/<type>')
def request_sales_summary(type):
    query = f"SELECT {type}, COUNT(*) AS num FROM supermarket_sales GROUP BY {type}"
    dataframe = pd.read_sql(query, conn)
    result = dataframe.to_dict(orient="records")
    return {"data": result}


if __name__ == "__main__":
    app.run()