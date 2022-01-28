from collections import Counter

def count_unq_words(ip_str):
    ip_str=ip_str.lower()
    ip_list=ip_str.split()
    op = Counter(ip_list)
    print(len(op))


ip_str="Count count words in a sentence, tear me to pieces"
count_unq_words(ip_str)


