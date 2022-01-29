
def none_replace(ls):
    t = 0
    rs = []
    for i in ls:
        if i is not None:
            rs.append(i)
            t = i
        else:
            rs.append(t)
    
    for i, x in enumerate(rs):
        if x == 0:
            rs[i] = None

    return rs

stuffies = [None, None, 1, 2, None, None, 3, 4, None, 5, None, None]

print(none_replace(stuffies))