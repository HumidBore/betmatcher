import re
# print(re.sub(r'\b\w{1,3}\b', '', 'AC MILAN FC BUBBA'))
# print(re.sub(r'(?:\b\w{,3}\s|\s\w{,3}\b|\b\w{,3}\b)', '', 'AC MILAN FC BUBBA'))

# match = re.sub(r'[.\(\)\[\]?!]', '', '[?]AC     F(C MIL)AN- FC BU!!BBA . b.') 
squadra1, squadra2 = re.sub(r'\b\w{1,3}\b', '', re.sub(r'[.\(\)\[\]?!]', '', '[?]AC     F(C MIL)AN- FC BU!!BBA . b Femm.') ).split('-', 1)
eventName = re.sub(r' +', ' ', f'{squadra1} - {squadra2}'.strip())  
print(eventName)

# squadra1, squadra2 = match.split('-', 1)
# print(squadra1)
# print(squadra2)


#remove substrings made of three or less characters
# squadra1 = re.sub(r'\b\w{1,3}\b|[.]', '', squadra1)
# squadra2 = re.sub(r'\b\w{1,3}\b|[.\[\]\(\)]', '', squadra2)
# print(squadra1)
# print(squadra2)
# eventName = f'{squadra1} - {squadra2}'
# print (eventName)
# eventName = eventName.strip()
# print (eventName)
# eventName = re.sub(' +',' ',eventName)
# print (eventName)
