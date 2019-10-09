import json

dic={'username':'15915765128','password':'87662638qjf'}
with open('joinquant_password.json','w') as f:
    json.dump(dic,f)
    pass

dic={'start_date':'2005-01-01','end_date':'2019-10-08'}
with open('start_end_date.json','w') as f:
    json.dump(dic,f)
