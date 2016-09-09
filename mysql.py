import pprint
import MySQLdb
import datetime

CLOUDSQL_PROJECT = 'dataminor5'
# CLOUDSQL_INSTANCE = 'dataminor-sql'
CLOUDSQL_INSTANCE = 'dataminor5:us-central1:dataminor-sql'

pp = pprint.PrettyPrinter(indent=4)

def get_db():
    return MySQLdb.connect(
        unix_socket='/cloudsql/%s' % CLOUDSQL_INSTANCE,
        user='root',
        db='currencies',
        passwd='grannyhamhat')

    # return MySQLdb.connect(user=u'root', passwd=u'mysql922', db=u'currencies', charset=u'utf8', use_unicode=True)

def remove_extraneous():
    db = get_db()
    PAGE_SIZE = 1000
    cur = db.cursor()

    since = datetime.datetime.now() - datetime.timedelta(hours=12)
    cur.execute("SELECT * FROM quotes WHERE captured_at > %s ORDER BY captured_at ASC", [since])
    res = cur.fetchmany(PAGE_SIZE)
    latest_quotes = {}
    ids_to_delete = []
    data = { 'removed_count': 0 }

    while (res.__len__() > 0):

        for result in res:
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

        if len(ids_to_delete) > 0:
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
    db.close()
    return data

def insert_quotes(source, quotes):
    db = get_db()
    cursor = db.cursor()
    for row in quotes:
        q = "INSERT INTO quotes (name, source, quote, captured_at) VALUES ('%s', '%s', '%s', '%s')" % (row[0], source, row[1], row[2])
        cursor.execute(q)
    db.commit()
    cursor.close()
    db.close()
