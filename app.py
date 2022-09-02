from flask import Flask,request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy_serializer import SerializerMixin
from sqlalchemy.sql import func
import time
import threading

from .imagine import imagine

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/test.db'
db = SQLAlchemy(app)

class Job(db.Model, SerializerMixin):
    id = db.Column(db.Integer, primary_key=True)
    config = db.Column(db.JSON)
    pending = db.Column(db.Boolean)
    created = db.Column(db.DateTime(timezone=True), server_default=func.now())
    result = db.Column(db.String, nullable=True)

db.create_all()

def serialize_jobs(jobs):
    return [x.to_dict() for x in jobs]


def get_pending_jobs():
    return Job.query.filter_by(pending=True).order_by(Job.created).all()


@app.route('/jobs',  methods=['GET', 'POST'])
def handle_jobs():
    """Jobs handler"""
    if request.method == 'GET':
        return serialize_jobs(Job.query.all())
    elif request.method == 'POST':
        job = Job(config=request.get_json(), pending=True)
        db.session.add(job)
        db.session.commit()
        db.session.refresh(job)
        return job.to_dict()


@app.route('/queue')
def get_queue():
    """Gets the current queue of jobs."""
    return serialize_jobs(get_pending_jobs())


@app.route('/')
def hello_world():
    return [x.to_dict() for x in Job.query.all()]


# Handle the jobs in a seperate worker thread.
def worker_thread():
    while True:
        jobs = get_pending_jobs()
        if len(jobs) > 0:
            job = jobs[0]
            print("Processing job", job)
            try:
                result = imagine(job.config)
                job.result = result
            except:
                pass
            job.pending = False
            db.session.commit()
        else:
            print("No jobs found, sleeping")
            time.sleep(5)

t = threading.Thread(target=worker_thread)
t.start()