def none_replace(ls):
    p = None
    return [p:=e if e is not None else p for e in ls]

stuffies = [None, None, 1, 2, None, None, 3, 4, None, 5, None, None]

print(none_replace(stuffies))