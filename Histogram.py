# def histogram( items ):
#     for n in items:
#         output = ''
#         times = n
#         while( times > 0 ):
#           output += '*'
#           times = times - 1
#         print(output)

# histogram([2, 3, 6, 5])

def histogram(list):
    for n in list:
      output = ''
      times = n 
      while(times > 0):
        output += '*'
        times = times - 1
      print(output)

histogram([4,2,1,5,100])

