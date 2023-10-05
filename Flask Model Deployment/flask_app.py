from flask import Flask

app = Flask(__name__)

@app.route('/') # allows /page_name route to the application

def home():
    return "This application was created by Ethan Feiza."

app.run(port=5000)