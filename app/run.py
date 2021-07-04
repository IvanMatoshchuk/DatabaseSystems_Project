import json
import os
from pathlib import Path

import plotly
import pandas as pd

from flask import Flask
from flask import render_template, request, jsonify

import sqlite3

app = Flask(__name__)




# index webpage displays cool visuals and receives user input text for model
@app.route('/')
@app.route('/index')
def index():
    
    return render_template('master.html')#, ids=ids, graphJSON=graphJSON)


# web page that handles user query and displays model results
@app.route('/go')
def go():
    # save user input in query
    query = request.args.get('query', '') 

    # load data
    path_to_db = os.path.join(Path(__file__).parent.parent, "data", "country_stat.db")
    conn = sqlite3.connect(path_to_db)  
    c = conn.cursor()

    df = pd.DataFrame(c.execute(query).fetchall(), columns=list(map(lambda x: x[0], c.description)))
  
    # create visuals
    # TODO: Below is an example - modify to create your own visuals
    try:
        graphs = [
            {
                'data': [
                    {'mode':"lines",
                    'name': "Json data",
                    "type": "scatter",
                    "x": df.Year.tolist(),
                    "y": df.iloc[:,1].tolist()}
                
                ],

                'layout': {
                    'title': 'Export in Mil. USD $',
                    'yaxis': {
                        'title': "Mil. $"
                    },
                    'xaxis': {
                        'title': "Year"
                    }
                }
                
            },

            {
                'data': [
                    {'mode':"lines",
                    'name': "Json data",
                    "type": "scatter",
                    "x": df.Year.tolist(),
                    "y": df.iloc[:,2].tolist()}
                
                ],

                'layout': {
                    'title': 'CO2 Emission in Mil. Ton.',
                    'yaxis': {
                        'title': "Mil. Ton."
                    },
                    'xaxis': {
                        'title': "Year"
                    }
                }
                
            }
            
        ]

        # encode plotly graphs in JSON
        ids = ["graph-{}".format(i) for i, _ in enumerate(graphs)]
        graphJSON = json.dumps(graphs, cls=plotly.utils.PlotlyJSONEncoder)



        # This will render the go.html Please see that file. 
        return render_template(
            'go.html',
            query=query,
            tables=[df.head(10).to_html(classes='data',  header="true")],
            ids=ids,
            graphJSON=graphJSON
        # classification_result=classification_results
        )
    except Exception:

        # This will render the go.html Please see that file. 
        return render_template(
            'go.html',
            query=query,
            tables=[df.head(10).to_html(classes='data',  header="true")],
            #ids=ids,
           #graphJSON=graphJSON
        # classification_result=classification_results
        )






def main():
    app.run(host='0.0.0.0', port=3001, debug=True)


if __name__ == '__main__':
    main()