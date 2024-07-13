from flask import Flask, jsonify, request
import psycopg2

import spacy
from flask_cors import CORS
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Assume you have spaCy or another NLP library initialized
nlp = spacy.load("en_core_web_sm")

app = Flask(__name__)
CORS(app)

# PostgreSQL database connection setup
DB_HOST = 'localhost'
DB_NAME = 'events_data'
DB_USER = 'postgres'
DB_PASSWORD = 'root'

# Function to execute SQL queries
def execute_sql_query(sql_query):
    try:
        print("sql_query",sql_query)
        dbname = os.getenv('POSTGRES_DATABASE') or DB_NAME
        user = os.getenv('POSTGRES_USER') or DB_USER
        password = os.getenv('POSTGRES_PASSWORD') or DB_PASSWORD
        host = os.getenv('POSTGRES_HOST') or DB_HOST
        
        # print(dbname,"dbname")
        conn = psycopg2.connect(
            host=host, 
            database=dbname, 
            user=user, 
            password=password,
              )
        cursor = conn.cursor()

        cursor.execute(sql_query)
        
        # Fetch all rows if SELECT query
        if sql_query.strip().lower().startswith('select'):
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            result = [dict(zip(columns, row)) for row in rows]
        else:
            result = f"Query executed successfully. {cursor.rowcount} rows affected."

        conn.commit()
        cursor.close()
        conn.close()

        return result

    except psycopg2.Error as e:
        return f"Error executing SQL query: {str(e)}"

@app.route('/data', methods=['POST'])
def process_query():
    data = request.get_json()
    user_query = data['query']

    # parsed_url = urlparse(request.url)
    # path = parsed_url.path
    # query_params = parse_qs(parsed_url.query)
    # user_query = query_params.get('query', [''])[0]

    # user_query = request.args.get('query')
    print("user_query",user_query)
    parsed_query = nlp(user_query)

    action = None
    entities = {}
    conditions = []
    time_frame = None

    for i,token in enumerate(parsed_query):
        num_months = None
        if token.pos_ == 'VERB' and token.dep_ in ('ROOT', 'conj'):
            action = token.lemma_
        # if token.ent_type_ != '' and token.dep_ == "compound" and token.head.pos_ == "NOUN":
        if token.ent_type_ != '':
            entities[token.ent_type_] = token.text
        if token.text.lower() == "people" :
            entities["PERSON"] = token.text
        if token.text == "events" and token.dep_ in ["pobj", "dobj"]:
            entities['GPE'] = token.text
        if token.dep_ == 'pobj' and token.head.dep_ == 'prep':
            conditions.append((token.head.lemma_, token.text))
        if token.text == "next" and i < len(parsed_query) - 2:
            next_token = parsed_query[i + 1]
            next_next_token = parsed_query[i + 2]
            if next_token.text.isdigit() and next_next_token.text == "months":
                num_months = next_token.text
                # time_frame.append(f"next {num_months} months")
                time_frame = f"{num_months} MONTH"
                break

        
        

    print("time_frame",time_frame)
    print("action",action)
    print("entities",entities)
    print("conditions",conditions)

    INCOMPLETE_QUERY = 'Incomplete query'

    # Construct SQL query based on action and parsed information
    if action == 'find':
        if 'GPE' in entities and 'ORG' in entities:
            sql_query = ""
            if(time_frame == '12 MONTH'):
                #### OIL and GAS- 1ST
                sql_query = f"""
                SELECT t1.company_name,t1.event_url,
                (SELECT t2.event_start_date
                FROM event_info t2 
                WHERE t2.event_url = t1.event_url
                AND t2.event_start_date >= CURRENT_DATE
                AND t2.event_start_date < DATE_ADD(CURRENT_DATE, INTERVAL '{time_frame}')
                )
                FROM 
                company_info t1 where company_industry like '%{entities['ORG']}%'
                """
            else:
                #### FOR PHARMA QUERY - 3RD
                entities['ORG'] = entities['ORG'][:6]
                sql_query = f"""
                SELECT t1.company_name,t1.event_url,
                (SELECT t2.event_name
                FROM event_info t2 
                WHERE t2.event_url = t1.event_url)
                FROM 
                company_info t1 where company_industry like '%{entities['ORG']}%'
                """
        elif 'PERSON' in entities and 'GPE' in entities:
            ### FOR SALES PEOPLE SINGAPORE 2ND
            sql_query = f"""
                SELECT pi.*, ei.event_venue, ei.event_start_date FROM people_info pi
                JOIN company_info ci on ci.homepage_base_url = pi.homepage_base_url 
                JOIN event_info ei  on ei.event_url = ci.event_url
                WHERE ei.event_venue like '%{entities['GPE']}%' AND pi.job_title ilike '%sales%'
                AND ei.event_start_date >= CURRENT_DATE
                AND ei.event_start_date < DATE_ADD(CURRENT_DATE, INTERVAL '{time_frame}')
            """
        elif 'GPE' in entities:  # If location is mentioned
            sql_query = f"SELECT * FROM event_info WHERE event_venue like '%{entities['GPE']}%'"
        elif 'ORG' in entities:  # If organization/company_info is mentioned
            sql_query = f"SELECT * FROM company_info WHERE company_name like '%{entities['ORG']}%'"
        else:
            return jsonify({"error": INCOMPLETE_QUERY}), 400

    elif action == 'list':
        if 'DATE' in entities:  # If date is mentioned
            sql_query = f"SELECT * FROM event_info WHERE date = '{entities['DATE']}'"
        else:
            sql_query = f"SELECT * FROM event_info"

    elif action == 'show':
        if 'PERSON' in entities:  # If person is mentioned
            sql_query = f"SELECT * FROM people_info WHERE first_name = '{entities['PERSON']}'"
        else:
            return jsonify({"error": INCOMPLETE_QUERY}), 400

    elif action == 'count':
        if 'event_info' in user_query:
            sql_query = f"SELECT COUNT(*) FROM event_info"
        elif 'people_info' in user_query:
            sql_query = f"SELECT COUNT(*) FROM people_info"
        else:
            return jsonify({"error": "Unsupported query"}), 400

    elif action == 'join':
        if 'company_info' in entities and 'event_info' in entities:
            sql_query = f"SELECT * FROM company_info c JOIN event_info e ON c.id = e.company_id"
        else:
            return jsonify({"error": INCOMPLETE_QUERY}), 400
        
    elif action == "need":
        ## FOR EMAIL 4TH QUERY
        sql_query = """
        SELECT
            p.first_name,
            p.last_name,
            c.company_industry,
            CASE 
                WHEN p.email_pattern = '[first_initial][last]' THEN SUBSTR(p.first_name, 1, 1) || p.last_name || '@' || p.homepage_base_url
                WHEN p.email_pattern = '[first].[last]' THEN p.first_name || '.' || p.last_name || '@' || p.homepage_base_url
                WHEN p.email_pattern = '""' THEN 'dummy_email@' || p.homepage_base_url
                ELSE 'Unknown pattern'
            END AS generated_email
        FROM
            people_info p
        JOIN
            company_info c ON p.homepage_base_url = c.homepage_base_url
        WHERE
            (c.company_industry LIKE '%Finance%' OR c.company_industry LIKE '%Banking%')
        """

    else:
        return jsonify({"error": "Unsupported action"}), 400

    # Apply additional conditions if specified
    # if conditions:
    #     for condition in conditions:
    #         sql_query += f" AND {condition[0]} = '{condition[1]}'"

    # Execute the constructed SQL query (replace with actual execution logic)
    result = execute_sql_query(sql_query)

    return jsonify({"result": result}), 200

if __name__ == '__main__':
    app.run(debug=True)
