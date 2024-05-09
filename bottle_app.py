from bottle import Bottle, run, response  # Import response here
import websocket
import json
import time
import csv

app = Bottle()

post_code = 2000
batch_size = 2000
max_offset = 6000
current_offset = 0

@app.route('/run')
def run_extraction():
    all_results = [] 


    def run_batch(current_offset,batch_size):    
            
        def extract_contact_info(data):
            results = []
            data = json.loads(data)
            try:
                q_matrix = data["result"]["qLayout"][0]["value"]["qHyperCube"]["qDataPages"][0]["qMatrix"]
                for row in q_matrix:
                    result = {
                        'First Name': row[0]["qText"] if "qText" in row[0] else "N/A",
                        'Last Name': row[1]["qText"] if "qText" in row[1] else "N/A",
                        'Email': row[2]["qText"] if "qText" in row[2] else "N/A",
                        'Phone Number': row[3]["qText"] if "qText" in row[3] else "N/A",
                        'Postcode': row[4]["qText"] if "qText" in row[4] else "N/A"
                    }
                    results.append(result)
            except Exception as e:
                print(f"An error occurred while extracting data: {e}")
            return results


        def fetch_next_batch():
            global current_offset
            create_hypercube_session_object_message = json.dumps({
                "delta": True,
                "handle": 1,
                "id": 15,
                "jsonrpc": "2.0",
                "method": "CreateSessionObject",
                "params": [{
                    "qHyperCubeDef": {
                        "qInitialDataFetch": [{
                            "qTop": current_offset,
                            "qHeight": batch_size,
                            "qWidth": 5
                        }],
                        "qInterColumnSortOrder": [0, 1],
                        "qDimensions": [
                            {"qDef": {"autoSort": False, "qSortCriterias": [{"qSortByAscii": 1}], "qFieldDefs": ["FirstName"]}, "qNullSuppression": True},
                            {"qDef": {"autoSort": False, "qSortCriterias": [{"qSortByAscii": 1}], "qFieldDefs": ["LastName"]}, "qNullSuppression": True},
                            {"qDef": {"qFieldDefs": ["Email"]}, "qNullSuppression": True},
                            {"qDef": {"qFieldDefs": ["Phone"]}, "qNullSuppression": True},
                            {"qDef": {"qFieldDefs": ["Postcode"]}, "qNullSuppression": True}
                        ],
                        "qSuppressZero": False,
                        "qSuppressMissing": False,
                        "qMode": "S",
                        "qStateName": "$"
                    },
                    "qInfo": {"qType": "mashup", "qId": "MURewpRpC"}
                }]
            })
            ws.send(create_hypercube_session_object_message)
            print(f"Sent CreateHyperCubeSessionObject request for batch starting at index {current_offset}")

            # Update the offset for the next batch
            current_offset += batch_size






        try:
            ws = websocket.create_connection("wss://qap.dss.gov.au/app/8b12168a-8fdf-4ca2-afe4-413db6420cf9?reloadUri=https%3A%2F%2Fwww.ndiscommission.gov.au%2Ffind-ndis-behaviour-support-practitioner")
            print("WebSocket connection established")
            
            
            open_doc_message = json.dumps({
                'delta': True,
                'handle': -1,
                "id": 1,
                "jsonrpc": "2.0",
                "method": "OpenDoc",
                "params": ["8b12168a-8fdf-4ca2-afe4-413db6420cf9", "", "", "", False],
                
            })
            
            ws.send(open_doc_message)
            
            print("Sent OpenDoc request")

            get_app_layout_message = json.dumps({
                "delta": True,
                "handle": 1,
                "id": 2,
                "jsonrpc": "2.0",
                "method": "GetAppLayout",
                "params": []
            })
            
            ws.send(get_app_layout_message)
            print("Sent GetAppLayout request")

            time.sleep(1)

            get_field_message = json.dumps({
                "delta": True,
                "handle": 1,
                "id": 3,
                "jsonrpc": "2.0",
                "method": "GetField",
                "params": ["FirstName", "$"]
            })
            
            ws.send(get_field_message)
            print("Sent GetField request")


            create_session_object_message = json.dumps({
                "delta": True,
                "handle": 1,
                "id": 7,
                "jsonrpc": "2.0",
                "method": "CreateSessionObject",
                "params": [{
                    "qHyperCubeDef": {
                        "qInitialDataFetch": [
                            {
                                "qHeight": 5,
                                "qWidth": 2
                            }
                        ],
                        "qDimensions": [
                            {
                                "qDef": {
                                    "qFieldDefs": [
                                        "ExtractDate"
                                    ]
                                },
                                "qNullSuppression": True,
                                "qOtherTotalSpec": {
                                    "qOtherMode": "OTHER_OFF",
                                    "qSuppressOther": True,
                                    "qOtherSortMode": "OTHER_SORT_DESCENDING",
                                    "qOtherCounted": {
                                        "qv": "5"
                                    },
                                    "qOtherLimitMode": "OTHER_GE_LIMIT"
                                }
                            },
                            {
                                "qDef": {
                                    "qFieldDefs": [
                                        "ExtractTime"
                                    ]
                                },
                                "qNullSuppression": True,
                                "qOtherTotalSpec": {
                                    "qOtherMode": "OTHER_OFF",
                                    "qSuppressOther": True,
                                    "qOtherSortMode": "OTHER_SORT_DESCENDING",
                                    "qOtherCounted": {
                                        "qv": "5"
                                    },
                                    "qOtherLimitMode": "OTHER_GE_LIMIT"
                                }
                            }
                        ],
                        "qSuppressZero": False,
                        "qSuppressMissing": False,
                        "qMode": "S",
                        "qStateName": "$"
                    },
                    "qInfo": {
                        "qType": "mashup",
                        "qId": "MUwmjmpNY"
                    }
                }]})

            
            time.sleep(1)
            
            ws.send(create_session_object_message)
            print("Sent CreateSessionObject request")

            search_message = json.dumps({
                "delta": True,
                "handle": 1,
                "id": 12,
                "jsonrpc": "2.0",
                "method": "SearchResults",
                "params": [
                    {
                        "qSearchFields": ["Postcode"],
                        "qContext": "Cleared"
                    },
                    [str(post_code)],
                    {
                        "qOffset": 0,
                        "qCount": 1
                    }
                ]
            })
            
            ws.send(search_message)
            print("Sent search request")



            fetch_next_batch()  # Fetch the first batch

            time.sleep(.2)


            get_layout_message = {
                "delta": True,
                "handle": 4,
                "method": "GetLayout",
                "params": [],
                "id": 16,
                "jsonrpc": "2.0"
            }

            ws.send(json.dumps(get_layout_message))
            print("Sent GetLayout request")
            
            while True:
                response = ws.recv()
                if response:
                    try:
                        if len(response) > 30000:
                            response_data = json.loads(response)
                            results = extract_contact_info(response)
                            return results
                    except Exception as e:
                        print("Failed to extract CSV names:", e)
                else:
                    break 

        except Exception as e:
            print("Failed to establish WebSocket connection:", e)
        finally:
            ws.close() 
            print("WebSocket connection closed")


    post_code = 2000
    current_offset = 0
    batch_size = 2000 

    for current_offset in range(0, 6000, batch_size):
        batch_results = run_batch(current_offset, batch_size)
        all_results.extend(batch_results)  # Collect all batch results

    response.content_type = 'application/json'
    print(json.dumps(all_results))
    return json.dumps(all_results)
    







# # Setting up the server to run on localhost at port 8080
# run(app, host='localhost', port=8080)
