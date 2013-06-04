from random import randint
from random import sample
from random import seed
from random import shuffle
from sys import argv
from sys import exit
from os import mkdir
import gzip

seed(37) # remove random seed if you want different results

try:
	NUM_ORDERS = int(argv[1])
	NUM_FILES = int(argv[2])
except:
	print 'usage: generate_receipts.py NUM_ORDERS NUM_FILES\n  example:  generate_receipts.py 10000 5'
	exit(1)

# this function generates the random string for whether or not a gift card was used
def gen_giftcard():
	if randint(0,39) == 37:   # 1/40 chance for a gift card to be used
		return '$' + str(randint(10, 120))  # if it is used, it is between $10 and $120
	else:
		return 'n/a' 

# this function generates a list of 1 to 12 random items from our item list
def gen_receipt_items():
	return sorted(sample(items, randint(1,12)))

# our randomly generated list of 30,000 items
# they cost between $1 and $100
items = [ (str(i), str(randint(100, 10000) / 100.0)) for i in range(1,30000) ]

# our randomly generated list of NUM_ORDERS orders
# the order number increments, starting at 10,000,000
# each order also has a random customer ID. Note, there are NUM_ORDERS/3 customers (meaning, on average, each customer has 3 orders)
orders = [ (str(randint(100000,100000 + NUM_ORDERS / 3)), \
	str(i), \
	gen_giftcard(), \
	gen_receipt_items(), \
		) for i in range(10000000, 10000000 + NUM_ORDERS) ]

# randomly generate the "splits" of the files
# these are the boundaries of which records go into which files
file_splits = [0] + sorted(sample(range(len(orders)), NUM_FILES - 1)) + [len(orders)]
file_parts = zip(file_splits[:-1], file_splits[1:])

# generate the data itself and dump it into a folder called "data"
mkdir('data')
for start, end in file_parts:
	fname = 'data/data' + str(start) + 'to' + str(end - 1) + '.gz'
	fp = gzip.open(fname, 'w')

	fp.write('CUSTOMER ID|ORDER NUMBER|GIFTCARD|RECEIPT ITEMS(...)\n')
	for order in orders[start:end]:
		fp.write('%s|%s|%s|' % order[0:3])
		fp.write('|'.join(item + ';' + cost + ';' + str(randint(1,4)) for item, cost in order[3]))
		fp.write('\n')

	fp.close()

