import requests
import psycopg2

def get_page(api, cur,conn):
    response = requests.get(f"{api}")

    if response.status_code == 200:
        send_data(response.json()["results"], cur, conn)
    else:
        print(f"Hello person, there's a {response.status_code} error with your request")


def get_data():
    (conn,cur) = connect_db()

    for i in range(101,1000):
        get_page("https://api.themoviedb.org/3/discover/movie?sort_by=popularity.desc&page=" + str(i) + "&api_key=0bf75b653483a5cf81fe54d645395de9", cur, conn)

    cur.close()
    conn.close()

def connect_db():
    conn = psycopg2.connect(
        host="<Adresse Host>",
        port="<Port>",
        database="<Database",
        user="<Username>",
        password="<Password>")
    # create a cursor
    cur = conn.cursor()
        
    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT * FROM movies')

    # display the PostgreSQL database server version
    db_version = cur.fetchall()
    print(db_version)
       
    return (conn,cur)

def send_data(data, cur,conn):
    for d in data:
        if "release_date" in d:
            if str(d["release_date"]) != '':
                d_release = "\'" + str(d["release_date"]) + "\'"
            else: 
                d_release = "NULL"
        else:
            d_release = "NULL"

        req = "INSERT INTO movies(adult,genre_ids,id,original_language,original_title,overview,release_date,title, vote_average) VALUES (" + str(d["adult"]) + ", ARRAY " + str(d["genre_ids"]) + "::integer[]," + str(d["id"]) + ",\'" + d["original_language"] + "\',\'" + d["original_title"].replace('\'','`') + "\',\'" + d["overview"].replace('\'','`') + "\'," + d_release + ",\'" + d["title"].replace('\'','`') + "\'," + str(d["vote_average"]) + ");"
        cur.execute(req)
        conn.commit()


get_data()