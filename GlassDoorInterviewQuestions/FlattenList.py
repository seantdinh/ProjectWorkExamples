
def flatten_list(ip_list, op_list=[]):
    for i in ip_list:
        if isinstance(i, list):
            flatten_list(i, op_list)
        else:
            op_list.append(i)
    return op_list

ip_list = [1, 2, [3, [4, [5, 6, [7,8,[9]]]]]]
print(flatten_list(ip_list))

