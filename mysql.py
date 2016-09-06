import pprint
import MySQLdb

CLOUDSQL_PROJECT = 'dataminor5'
# CLOUDSQL_INSTANCE = 'dataminor-sql'
CLOUDSQL_INSTANCE = 'dataminor5:us-central1:dataminor-sql'

pp = pprint.PrettyPrinter(indent=4)

db = MySQLdb.connect(
    unix_socket='/cloudsql/%s' % CLOUDSQL_INSTANCE,
    user='root',
    passwd='grannyhamhat')

# db = MySQLdb.connect(user=u'root', passwd=u'mysql922', db=u'currencies', charset=u'utf8', use_unicode=True)

cur = db.cursor()
cur.execute('CREATE DATABASE IF NOT EXISTS currencies');
cur.execute('USE currencies');
cur.execute("""
    CREATE TABLE IF NOT EXISTS quotes (
      id int(11) unsigned NOT NULL AUTO_INCREMENT,
      source varchar(16) NOT NULL,
      name varchar(24) NOT NULL,
      quote decimal(20,10) NOT NULL,
      captured_at timestamp NULL DEFAULT NULL,
      PRIMARY KEY (id),
      UNIQUE KEY name (name,source,quote,captured_at)
    ) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
""")
cur.close()
db.commit()

def remove_extraneous():
    PAGE_SIZE = 1000
    cur = db.cursor()
    cur.execute("SELECT * FROM quotes ORDER BY captured_at ASC")
    res = cur.fetchmany(PAGE_SIZE)
    latest_quotes = {}
    ids_to_delete = []
    data = { 'removed_count': 0 }

    while (res.__len__() > 0):

        for result in res:
            pp.pprint(result)
            row_id = result[0]
            source = result[1]
            name = result[2]
            quote = result[3]
            key = "%s__%s" % (source, name)
            if key in latest_quotes and latest_quotes[key][0][1] == quote:
                # append and remove the second, if this is the third...
                latest_quotes[key].append((row_id, quote))
                if len(latest_quotes[key]) > 2:
                    ids_to_delete.append(latest_quotes[key].pop(1)[0])
            else:
                latest_quotes[key] = [(row_id, quote)]

        print 'checking for deleting...'
        if len(ids_to_delete) > 0:
            print 'deleting...'
            pp.pprint(ids_to_delete)
            delete_query = 'DELETE FROM quotes WHERE id IN (%s)'
            in_p = ', '.join(map(lambda x: '%s', ids_to_delete))
            delete_query = delete_query % in_p
            delete_cursor = db.cursor()
            delete_cursor.execute(delete_query, ids_to_delete)
            db.commit()
            delete_cursor.close()
            data['removed_count'] = data['removed_count'] + len(ids_to_delete)
            del ids_to_delete[:]

        res = cur.fetchmany(PAGE_SIZE)

    cur.close()
    return data

def insert_quotes(source, quotes):
    # pp.pprint(quote)
    cursor = db.cursor()
    for row in quotes:
        q = "INSERT INTO quotes (name, source, quote, captured_at) VALUES ('%s', '%s', '%s', '%s')" % (row[0], source, row[1], row[2])
        cursor.execute(q)
    db.commit()
    cursor.close()
