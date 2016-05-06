def go():
    sample_definition = {
       "date_filter":{

       },
       "eligible_customer_filter":{
          "or_filters":[
             {
                "and_filters":[
                   {
                      "filter":{
                         "date_filter":{

                         },
                         "duration_filter":{
                            "duration":{
                               "count":30,
                               "units":"days"
                            },
                            "enabled":False
                         },
                         "independent_if_reused":True,
                         "steps":[
                            {
                               "filter":[

                               ],
                               "type":"app_action"
                            }
                         ],
                         "type":"funnel_steps"
                      },
                      "negate":False
                   }
                ]
             }
          ]
       },
       "event_filters":[
          {
             "filter":[

             ],
             "type":"app_action"
          },
          {
             "filter":[

             ],
             "type":"first_dashboard_created"
          }
       ],
       "mode":"simple",
       "target_customer_filter":{
          "or_filters":[
             {
                "and_filters":[
                   {
                      "filter":{
                         "date_filter":{

                         },
                         "duration_filter":{
                            "duration":{
                               "count":30,
                               "units":"days"
                            },
                            "enabled":False
                         },
                         "independent_if_reused":True,
                         "steps":[
                            {
                               "filter":[

                               ],
                               "type":"create_project"
                            }
                         ],
                         "type":"funnel_steps"
                      },
                      "negate":False
                   },
                   {
                      "filter":{
                         "attribute":{
                            "property":"plan",
                            "type":"property"
                         },
                         "constraint":{
                            "operands":[
                               "Event packs"
                            ],
                            "operator":"equals",
                            "type":"string"
                         },
                         "type":"attribute"
                      },
                      "negate":False
                   }
                ]
             }
          ]
       }
    }

    print sample_definition['target_customer_filter']['or_filters'][0]['and_filters'][0]['filter']['steps'][0]['type']

go()