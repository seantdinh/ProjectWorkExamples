import collections

d = collections.defaultdict(int)
for c in thestring:
    d[c] += 1


for c in sorted(d, key=d.get, reverse=True):
  print '%s %6d' % (c, d[c])


#   https://stackoverflow.com/questions/991350/counting-repeated-characters-in-a-string-in-python