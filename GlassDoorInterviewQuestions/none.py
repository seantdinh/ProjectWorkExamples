d = [1,'q','3', None, 'temp']
answer = []
for i in d:
    if i != None:
        answer.append(i)
    else:
        answer.append('sean')
print(answer)