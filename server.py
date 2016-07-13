from flask import Flask, request, jsonify
import psycopg2, psycopg2.extras

# DSN location of the AWS - RDS instance
DB_DSN = "host= dbname= user= password="
# DB_DSN = "host=localhost dbname=kth user=kth"
app = Flask(__name__)


@app.route('/')
def default():

    output = {"message": "Welcome to the test app!"}

    return jsonify(output)


@app.route('/reviews/most_helpful_review')
def get_most_helpful_review():
    """
    finds the review with the highest proportion of votes indicating
    other users found his/her review helpful and with the highest total
    number of votes. Returns the review text, user who submitted the 
    review, and the item asin for which the review was written.
    """
    out = dict()
    sql = "SELECT b.title title, a.reviewer_name reviewer_name, \
               a.review most_helpful_review from \
            (select asin, review, helpful_score, reviewer_name, \
                  total_helpful_votes from reviews) a \
            left join \
            (select asin, title from books) b \
            USING(asin) \
            order by a.helpful_score desc, a.total_helpful_votes desc \
            limit 1;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()
        
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

@app.route('/reviews/least_helpful_review')
def get_least_helpful_review():
    """
    finds the review with the smallest proportion of votes indicating
    other users found his/her review helpful and with the highest total
    number of votes. Returns the review text, user who submitted the 
    review, and the item asin for which the review was written.
    """

    out = dict()
    sql = "SELECT b.title title, a.reviewer_name reviewer_name, \
               a.review least_helpful_review from \
            (select asin, review, helpful_score, reviewer_name, \
                  total_helpful_votes from reviews) a \
            left join \
            (select asin, title from books) b \
            USING(asin) \
            where a.helpful_score = 0 \
            order by a.total_helpful_votes desc \
            limit 1;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

@app.route('/reviews/most_concise_good_review')
def get_most_concise_helpful_review():
    """
    finds the review with the least number of characters and with the 
    highest total number of votes. Returns the review text, user who 
    submitted the review, and the item asin for which the review was 
    written.
    """

    out = dict()
    sql = "SELECT b.title title, a.reviewer_name reviewer_name, \
               a.review most_concise_good_review FROM \
            (SELECT asin, review, helpful_score, reviewer_name, \
                  total_helpful_votes, len_review_character_count FROM reviews) a \
            left join \
            (SELECT asin, title from books) b \
            USING(asin) \
            where a.len_review_character_count < 40 \
            ORDER BY a.helpful_score DESC, a.total_helpful_votes desc \
            LIMIT 1;"
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()
        
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

@app.route('/books/most_expensive_book')
def get_most_expensive_book():
    """
    finds the book title with the highest price. 
    Returns the title and price of the book.
    """

    out = dict()
    sql = "SELECT title, price \
            from books \
            order by price desc \
            limit 1;" 
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()
        
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

@app.route('/books/cheapest_book')
def get_cheapest_book():
    """
    finds the book title with the lowest price. 
    Returns the title and price of the book.
    """

    out = dict()
    sql = "SELECT title, price \
            from books \
            where price > -1 \
            order by price asc \
            limit 1;" 
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()
        
    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

@app.route('/reviews/earliest_review')
def get_earliest_review():
    """
    finds the book with the oldest review in the database. 
    Returns the title, review and date of the review.
    """
    out = dict()
    sql = "SELECT a.title, b.review_time, b.review from \
            (select asin, title from books) a \
             left join \
            (select asin, review, review_time from reviews) b \
             using(asin) \
             where review_time > to_date('1900-01-01', 'YYYY-MM-DD') \
             order by review_time \
             limit 1;" 
    try:
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql)
        out = cur.fetchall()

    except psycopg2.Error as e:
        print e.message
    else:
        cur.close()
        conn.close()
        out = out[0]

    return jsonify(out)

if __name__ == "__main__":    
    app.run(host='0.0.0.0') 
