def flattenlist(list):
    # output = [item for sublist in list for item in sublist]
    output = []
    for sublist in list:
        for item in sublist:
            output.append(item)

    print(output)

flattenlist([1,2,[3,4 [5],[6,7,[8,[9]]]]])