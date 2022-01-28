#3. Remove dupes from a list

def remove_dupes(ip_list):
    op_dict={}
    op_list=[]
    for i in ip_list:
        if i not in op_dict.keys():
            op_dict[i] = 1
            op_list.append(i)
    return op_list

ip_list=[1,1,1,2,2]
print(remove_dupes(ip_list))

print(list(set(ip_list)))

def remove(list):
    real = []
    for i in list:
        if i not in real:
            real.append(i)
    return real

print(remove(ip_list))