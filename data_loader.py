from datetime import datetime
import json
import psycopg2, psycopg2.extras


# DSN location of the AWS - RDS instance
DB_DSN = ""

# location of the input data file
books_data   = 'meta_Books.json'
reviews_data = 'reviews_Books.json'


###############################################################################
#
# Some functions to help transform data
#
###############################################################################

def get_asin(obj):
    'get book asin from json object. returns asin as a string'
    try:
        asin = obj['asin']
    except KeyError:
        return ''
    else:
        return asin

def get_category(obj):
    'get category of item from json object. returns category as a string'
    try:
        category = obj['categories']
    except KeyError:
        return ''
    else:
        return category[0][0]

def get_title(obj):
    'get item title from json object. return title as a string'
    try:
        title = obj['title']
    except KeyError:
        return ''
    else:
        return title

def get_description(obj):
    'get item description from json object. return description as a string'
    try:
        description = obj['description']
    except KeyError:
        return ''
    else:
        return description

def get_price(obj):
    '''get price of item from json object. return price as a float. 
    return -1 if price is not present in the object'''
    try:
        price = obj['price']
    except KeyError:
        return -1
    else:
        return price

def get_imurl(obj):
    'get image url from json object. return '' if url is not present'
    try:
        imurl = obj['imUrl']
    except KeyError:
        return ''
    else:
        return imurl

def get_sales_rank(obj):
    'get sales rank from json object. return ('', -1) if sales rank is not present'
    try:
        sales_rank = obj['salesRank']
    except KeyError:
        return ('', -1)
    else:
        sr_key = sales_rank.keys()[0]
        sr_val = sales_rank[sr_key]
        return (sr_key, sr_val)

def get_also_viewed(obj):
    '''get items that were also viewed from json object. return an array 
       with all asins that were also viewed, or an empty array if there were
       no such asins.
    '''
    try:
        related = obj['related']
    except KeyError:
        return []
    else:
        try:
            also_viewed = related['also_viewed']
        except KeyError:
            return []
        else:
            return also_viewed

def get_also_bought(obj):
    '''get items that were also bought from json object. return an array 
       with all asins that were also bought, or an empty array if there were
       no such asins.
    '''
    try:
        related = obj['related']
    except KeyError:
        return []
    else:
        try:
            also_bought = related['also_bought']
        except KeyError:
            return []
        else:
            return also_bought

def get_bought_together(obj):
    '''get items that were bought together from json object. return an array 
       with all asins that were bought together, or an empty array if there were
       no such asins.
    '''    
    try:
        related = obj['related']
    except KeyError:
        return []
    else:
        try:
            bought_together = related['bought_together']
        except KeyError:
            return []
        else:
            return bought_together

def get_buy_after_viewing(obj):
    '''get items that were bought after viewing from json object. return an array 
       with all asins that were bought after viewing, or an empty array if there were
       no such asins.
    '''
    try:
        related = obj['related']
    except KeyError:
        return []
    else:
        try:
            buy_after_viewing = related['buy_after_viewing']
        except KeyError:
            return []
        else:
            return buy_after_viewing

def get_review_time(obj):
    '''get date from json object. return date in the form m-d-Y if the date is present,
       else return the date 1900-01-01.
    '''
    try:
        review_time = obj['reviewTime']
    except KeyError:
        return '1900-01-01 00:00:00'
    else:
        return str(datetime.strptime(review_time, "%m %d, %Y")).split()[0]

def get_helpful_score(obj):
    try:
        helpful = obj['helpful']
    except KeyError:
        return -1
    else:
        return helpful[0] / float(helpful[1]) if helpful[1] else -1

def get_reviewer_name(obj):
    try:
        name = obj['reviewerName']
        return name
    except KeyError:
        name = ''



###############################################################################
#
#
#
###############################################################################


def transform_books_data(file_path):
    data = []
    with open(file_path) as f:
        for line in f:
            obj               = json.loads(line)
            asin              = get_asin(obj)
            title             = get_title(obj)
            description       = get_description(obj)
            category          = get_category(obj)
            price             = get_price(obj)
            imurl             = get_imurl(obj)
            also_viewed       = get_also_viewed(obj)
            also_bought       = get_also_bought(obj)
            bought_together   = get_bought_together(obj)
            buy_after_viewing = get_buy_after_viewing(obj)

            sales_rank        = get_sales_rank(obj)
            sales_rank_category = sales_rank[0]
            sales_rank_code   = sales_rank[1]
            data.append((asin, title, description, category, price, imurl, also_viewed, also_bought, 
                         bought_together, buy_after_viewing, sales_rank_category, sales_rank_code))
    return data

def transform_review_data(file_path):
    """
    :param filename: the filename of the data that will be transformed
    :return: list of tuples to be inserted into the db
    """
    review_data = []    
    with open(file_path) as f:
        for line in f:
            obj = json.loads(line)
            asin             = obj['asin']
            helpful_score    = get_helpful_score(obj)
            overall          = obj['overall']
            review_text      = obj['reviewText']
            len_review_character_count = len(review_text)
            review_time      = get_review_time(obj)
            name             = get_reviewer_name(obj)
            summary          = obj['summary']
            unix_review_time = obj['unixReviewTime']
            helpful_count, total_helpful_votes = obj['helpful']

            review_data.append((asin, helpful_count, total_helpful_votes, helpful_score, overall, 
                                review_text, len_review_character_count, review_time, name, summary, unix_review_time))
    return review_data

def drop_books_table():
    """
    drops the table 'books' if it exists
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS books;")
        con.commit()
    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        con.close()

def drop_reviews_table():
    """
    drops the table 'reviews' if it exists
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("DROP TABLE IF EXISTS reviews;")
        con.commit()
    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        con.close()



def create_books_table():
    """
    creates a postgres table with columns: 
      asin, a unique item identifier
      title, item's listing title
      len_title, number of characters in the title
      description, item description 
      len_description, number of characters in the item desription
      category, the item category 
      price, item price 
      imurl, image url 
      also_viewed, the asin codes of items that were also viewed 
      also_bought, the asin codes of items that were also bought 
      bought_together, the asin codes of items that were bought together 
      buy_after_viewing, the asin codes of item that were bought after viewing 
      sales_rank_category, sales rank category 
      sales_rank_code, sales_rank_code
      len_also_viewed, number of items that were also viewed 
      len_also_bought, number of items that were also bought 
      len_bought_together, number of items bought together 
      len_buy_after_viewing, number of items bought after viewing 
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("create table books (      \
                       asin text,               \
                       title text,               \
                       len_title int,             \
                       description text,           \
                       len_description int,         \
                       category text,                \
                       price float,                   \
                       imurl text,                     \
                       also_viewed text array,          \
                       also_bought text array,           \
                       bought_together text array,        \
                       buy_after_viewing text array,       \
                       sales_rank_category text,            \
                       sales_rank_code bigint,               \
                       len_also_viewed int,                   \
                       len_also_bought int,                    \
                       len_bought_together int,                 \
                       len_buy_after_viewing int                 \
                       ); "  
                    )
        con.commit()
    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        con.close()

def create_reviews_table():
    """
    creates a postgres table with columns: 
      asin, a unique item identifier
      helpful_count, integer indicating how many users found this review helpful
      total_helpful_votes, integer indicating total votes on whether review was helpful
      helpful_score, ratioof helpful_count / total_helpful_votes
      overall, overall score 
      review, review text
      len_review_character_count, number of characters in the review text
      review_time, time review was submitted
      reviewer_id, reviewer id
      reviewer_name, reviewer name
      summary, summary of review
    :return:
    """
    try:
        con = psycopg2.connect(dsn=DB_DSN)
        cur = con.cursor()
        cur.execute("create table reviews (     \
                       asin text,                \
                       helpful_count int,         \
                       total_helpful_votes int,    \
                       helpful_score float,         \
                       overall int,                  \
                       review text,                   \
                       len_review_character_count int, \
                       review_time date,                \
                       reviewer_id text,                 \
                       reviewer_name text,                \
                       summary text                        \
                       ); "  
                    )
        con.commit()
    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        con.close()

def insert_books_data(data):
    """
    inserts the data using execute many
    :param data: a list of tuples with order ...
    :return:
    """
    try:
        sql = "INSERT INTO books VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()

    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        conn.close()

def insert_reviews_data(data):
    """
    inserts the data using execute many
    :param data: a list of tuples with order ...
    :return:
    """
    try:
        sql = "INSERT INTO reviews VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        conn = psycopg2.connect(dsn=DB_DSN)
        cur = conn.cursor()
        cur.executemany(sql, data)
        conn.commit()

    except psycopg2.Error as e:
        print e.message

    else:
        cur.close()
        conn.close()

if __name__ == '__main__':
    # running this program as a main file will perform ALL the ETL
    # it will extract and transform the data from it file

    print "transforming data"
    books_data = transform_books_data(books_data)
    reviews_date = transform_reviews_data(reviews_data)

    # drop the db
    print "dropping table"
    drop_books_table()
    drop_reviews_table()

    # create the db
    print "creating table"
    create_books_table()
    create_reviews_table()

    # insert the data
    print "inserting data"
    insert_books_data(books_data)
    insert_reviews_data(reviews_data)
