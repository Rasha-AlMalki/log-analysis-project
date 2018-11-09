# Database code for the DB news..

#!/usr/bin/env python3

import psycopg2

DBNAME = "news"
def logs_analysis(sql_request):
    try:
        db = psycopg2.connect(database=DBNAME)
        c = db.cursor()
    except:
        raise Exception("Please check your database connections.")
    c.execute(sql_request)
    results = c.fetchall()
    db.close()
    return results

def popular_articles():
    popular_articles = logs_analysis("SELECT * FROM popular_articles;")
    print("1. What are the most popular three articles of all time?")
    i=1
    for num in popular_articles:
        print "\t", i, ") \"", num[0], "\" -- ", num[1], " views"
        i += 1

popular_articles()

def popular_authors():
    popular_authors = logs_analysis("""SELECT authors.name, COUNT(*) AS num
            FROM authors, articles, log
            WHERE log.status='200 OK'
            AND authors.id = articles.author
            AND articles.slug = substr(log.path, 10)
            GROUP BY authors.name
            ORDER BY num DESC;
            """)
    print("\n2. Who are the most popular article authors of all time?")
    i=1
    for num in popular_authors:
        print "\t", i, ") \"", num[0], "\" -- ", num[1], " views"
        i += 1

popular_authors()

def request_errors():
    request_errors = logs_analysis("""SELECT to_char(date, 'FMMonth FMDD, YYYY'),
        (error/result) * 100 AS ratio
        FROM (SELECT time::date AS date,
        COUNT(*) AS result,
        sum((status != '200 OK')::int)::float AS error
        FROM log GROUP BY date) AS errors
        WHERE error/result > 0.01;""")
    print("\n3. On which days did more than 1% of requests lead to errors?")
    
    for num in request_errors:
        print "\t", "\"" , num[0], "\" -- ", num[1],"%" " errors"
       
request_errors()
