
from collections import Counter

def count_occurance(sentance):
    lowercase = ip_str.lower()
    answer = ''
    counting = Counter(lowercase)        
    print(counting)

    for i in lowercase:
        if i != ' ' and i != ',':
            answer += str(i)+str(counting[i])
        else:
            answer += ' '
    return answer


ip_str="Count count words in a sentence, tear me to pieces"
print(count_occurance(ip_str))
