import os
from datetime import datetime

dir = os.path.dirname(os.getcwd()+'/users/')
print(dir)
try:
    os.stat(dir)
except:
    os.mkdir(dir) 
    print('make directory')

all_users = {}
for f in os.listdir(os.getcwd()):
    if f.endswith(".csv"): 
        split = str(f).split('_')
        code = split[1]
        print('[{}] Processing {} - {}'.format(datetime.now(), code, f))
        users = {}
        with open(f, 'r', encoding='utf8') as fr:
            for line in fr:
                split = line.split(',')
                if len(split) < 2:
                    continue
                try:
                    uid = int(split[1])
                except:
                    continue
                # This file users
                f_uid = users.get(uid)
                if f_uid is None:
                    users[uid] = 1
                else: 
                    users[uid] = f_uid + 1
                # All users
                f_all = all_users .get(uid)
                if f_all is None:
                    all_users[uid] = 1
                else:
                    all_users[uid] = f_all + 1
            with open(dir + '/' + str(code) + '.csv', 'w') as fw:
                for uid, f_uid in users.items():
                    fw.write('{},{}\n'.format(uid,f_uid))
        users.clear()

print('[{}] Processing all files'.format(datetime.now()))
with open(dir + '/#ALL.csv', 'w') as fw_all:
    for uid, f_all in all_users.items():
        fw_all.write('{},{}\n'.format(uid,f_all))

print('[{}] Program finished'.format(datetime.now()))