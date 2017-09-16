import psycopg2
import sys
import logging
from time import time

logging.basicConfig(filename='queries_tester.log', format="%(asctime)s:%(levelname)s:%(message)s", level=logging.NOTSET)

if len(sys.argv) < 2:
    print('Usage: {} host=127.0.0.1 port=5432 dbname=DBname user=User password=Pass'.format(__file__))
    exit(1)

connection_string = ' '.join(sys.argv[1:])

try:
    conn = psycopg2.connect(connection_string)
except:
    print('I am unable to connect to the data')
    exit(2)

logging.info('Connected to: {}'.format(connection_string))

cur = conn.cursor()

query = input('Query:').strip()
if len(query) == 0:
    print("Query can't be empty!")
    logging.fatal("Query can't be empty!")
    exit(3)
logging.info('Query:{}'.format(query))

try:
    repeats = int(input('How many times:'))
except:
    print('Incorrect number!')
    logging.fatal('Incorrect number of repeats!')
    exit(4)
logging.info('repeats: {}'.format(repeats))


try:
    multiply_query_by = int(input('Multiply query by:'))
except:
    print('Incorrect number!')
    logging.fatal('Multiply value must be an integer!')
    exit(5)
logging.info('Multiply query by: {}'.format(multiply_query_by))

if query[-1] != ';':
    query+=';'
query *= multiply_query_by

try:
    do_commit = int(input('Do commits after N queries:'))
except:
    print('Incorrect number!')
    logging.fatal('Commits must be an integer!')
    exit(6)
logging.info('Do commits after N queries: {}'.format(do_commit))


start_time = time()

for attempt in range(1, repeats+1):
   try:
      cur.execute("""
                 {}
                """.format(query))
   except:
      print('Incorrect query!')
      logging.critical('Incorrect query:{}'.format(1000 + attempt))
      exit(1000+attempt)

   if not attempt % do_commit:
      conn.commit()
#else:
#	conn.commit()

end_time = time()

working_time = end_time - start_time
logging.info('Queries time: {}'.format(working_time))
print('Queries time: {}'.format(working_time))

conn.close()

