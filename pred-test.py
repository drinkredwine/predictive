import argparse
import json
import requests
from time import sleep, time

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test predictions')
    parser.add_argument('--instance', type=str)
    parser.add_argument('--project_id', type=str)
    parser.add_argument('--definition', type=str)
    parser.add_argument('--route', type=str)

    args = parser.parse_args()
    definition = json.loads(args.definition)
    print('params and definition')
    print(args.instance, args.project_id, args.route)
    print(definition)

    with open("/tmp/prediction.log", "a") as f:
        f.writelines(["New run:", args.definition, args.definition, args.route, args.instance, args.project_id])

    # updating progress
    requests.post(args.route, json={'progress': 0.5})
    sleep(5)

    # reporting an error
    #requests.post(args.route, json={'error': 'Not enough milk'})

    # reporting result
    #with open('/data/labs/prediction-test/fake_result.json') as f:
    #    fake_result = json.loads(f.read())



    results = {
        'decision_tree':{
            'good':11,
            'bad':10,
            'name':0,
            'allowed_attributes':['a1','a2'],
            'rules':"",
            'expression':"",
            'children':""
        },
        'statistics':{
            "evaluation_auc":0.0000000000000000,
            "evaluation_lift":[
             {
                "y":0,
                "x":0
             }
            ],
            "training_auc":0.9856459330143540,
            "training_lift":[
             {
                "y":0,
                "x":0
             },
             {
                "y":2.8181818181818188,
                "x":3.2258064516129030
             }]
        }
    }
    fake_result = json.loads("")

    print('fake result')
    print(fake_result)
    result = requests.post(args.route, json={'progress': 1, 'result': fake_result})

    print('request result')
    print(result)
