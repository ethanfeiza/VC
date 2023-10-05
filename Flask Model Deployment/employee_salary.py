import numpy as np
from flask import Flask, request, render_template
import pickle

app = Flask(__name__)
my_model = pickle.load(open('my_model.pkl', 'rb'))

@app.route('/')
def home():
    return render_template('my_index.html')

@app.route('/predict', methods=['POST'])
def predict():
    int_features = [float(x) for x in request.form.values()]
    final_features = [np.array(int_features)]
    prediction = my_model.predict(final_features)

    output = round(prediction[0], 2)

    return render_template('my_index.html', prediction_text='Estimated Employee Salary: ${}'.format(output))

if __name__ == "__main__":
    app.run()