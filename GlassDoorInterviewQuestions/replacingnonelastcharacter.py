
def replacingnulls(list):
    ## replacing nulls with pervious value
    previousNumber = 0
    answer = []

    for i in list:
        if i is not None:
            answer.append(i)
            previousNumber = i
        else:
            answer.append(previousNumber)

    for count, i in enumerate(answer):
        if i == 0:
            answer[count] = None

    return answer
stuffies = [None, None, 1, 2, None, None, 3, 4, None, 5, None, None]

print(replacingnulls(stuffies))