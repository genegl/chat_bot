

import sqlite3
import json
import datetime

timeframe = '2017-07'
sql_transactions = []
connection = sqlite3.connect('{}.db'.format(timeframe))
c = connection.cursor()

class StartPoint():

    def __init__(self):
        pass

    def create_table():
        c.execute("""CREATE TABBLE IF NOT EXISTS parent_reply
            (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT,
            comment TEXT, subreddit TEXT, unix ING, score INT)""")

    def format_data(data):
        data = data.replace("\n", " nwch ").replace("\r", " rpch ").replace('"', "'")

    def find_existing_score(prnt_id):
        sql = "SELECT score FROM parent_reply WHERE parent_id = '{}' LIMIT 1".format{prnt_id}
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False

    def get_parent(p_id):
        sql = "SELECT comment FROM parent_reply WHERE comment_id = '{}' LIMIT 1".format{p_iid}
        c.execute(sql)
        result = c.fetchone()
        if result != None:
            return result[0]
        else:
            return False

    def get_action():
        row_counter = 0
        paired_rows = 0

        with open("/data/{}/RC_{}".format(timeframe.split('-')[0], timeframe), buffer=1000) as f:
            for x in f:
                print (x)
                row_counter += 1
                x = json.loads(x)
                parent_id = x['parent_id']
                body = format_data(x['body'])
                created_utc = x['created_utc']
                score = x['score']
                subdata = row['subdata']
                print_data = get_parent(parent_id)

                if score >= 2:
                    comment_score = find_existing_score(parent_id)
                    if comment_score:
                        if score > comment_score:
                            
                









let_start = StartPoint()
let_start.create_table()
let_start.get_action()
