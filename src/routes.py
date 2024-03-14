from app import app, db
from src import vendedores, test
import pandas as pd
from src.app_db import *
import random

@app.route('/')
def index():
 return '<h1>Hello World!</h1>'

@app.route(f'/reports/<id>')
def reports_func(id):
    df = vendedores.generate_by_campaign(id)
    return '<h1>You got it!</h1>'


@app.route(f'/test/<id>')
def test_func(id):
     # adds a new report to the list
    new_report= Reports(id = random.randint(1, 20000228),title= 'test for test',report_type='test', request_date='2000-28-02 08:45:00' )
    db.session.add(new_report)
    try:
        db.session.commit()
        print('Commited!')
    except Exception as err:
       print(str(err))
       db.session.rollback()

    df = test.test(id)
    return '<h1>You got it!</h1>'


@app.route(f'/vendedores/<id>')
def vendedores_func(id):
    df = vendedores.generate_by_campaign(id)
    return '<h1>You got it!</h1>'
