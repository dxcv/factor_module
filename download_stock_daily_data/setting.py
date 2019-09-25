import json

dic={'username':'15915765128','password':'87662638qjf'}
with open('joinquant_password.json','w') as f:
    json.dump(dic,f)
    pass

dic={'start_date':'2005-01-01'}
with open('start_date.json','w') as f:
    json.dump(dic,f)
