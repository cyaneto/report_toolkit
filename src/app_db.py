
from app import db

class Reports(db.Model):
    id = db.Column(db.Integer, primary_key = True)
    title = db.Column(db.String(255), index = True, unique = False)
    report_type =  db.Column(db.String(50), index = True, unique = False)
    request_date = db.Column(db.String(19), index = True, unique = False)

