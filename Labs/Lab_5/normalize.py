
# sample line:
#   116091|10000001|n/a|13407;64.71;2|13884;95.4;1|14781;70.35;1|19722;96.47;1|20846;90.23;2|9917;93.75;1

from sys import stdin


for line in stdin:
	if line.startswith('CUSTOMER ID'):
		# skip this line if it is a column header
		continue

	# split the line 3 times on pipe (into 4 items)
	# this way, the last column of variable length is in one field
	split_line = line.strip().split('|',3)

	# fix the gift card column
	if split_line[2] == 'n/a':
		# change n/a to 0
		split_line[2] = '0'
	else:
		# remove first character (the '$')
		split_line[2] = split_line[2][1:]

	# get the receipt line items
	items = split_line[3].split('|')

	# for each item, we'll print out a new line:
	for item in items:
		item_num, cost, amount = item.split(';')
		print '%s\t%s\t%s\t%s\t%s\t%s' % (split_line[0], split_line[1], split_line[2], item_num, cost, amount)

