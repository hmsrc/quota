'''
quota.py
5/26/14
'''

import os, grp
import ConfigParser as parser
import MySQLdb as sql
import subprocess

def sizeof_fmt(num):
    for x in [' b','Kb','Mb','Gb']:
        if num < 1024.0:
            return '%3.1f%s' % (num, x)
        num /= 1024.0
    return '%3.1f%s' % (num, 'Tb')

# read in MySQL config
config = parser.ConfigParser()
config.read('my.cnf')

# get parameters
sql_host = config.get('client', 'host')
sql_db = config.get('client', 'database')

sql_user = config.get('quota', 'user')
sql_passwd = config.get('quota', 'password')

sql_db_conn = sql.connect(host=sql_host,
                          user=sql_user,
                          passwd=sql_passwd,
                          db=sql_db)

sql_db_cursor = sql_db_conn.cursor()

query = '''
SELECT
qp.path AS path,
qcl.name AS cluster,
qc.type AS type,
qtn.id AS id,
qtn.name AS name,
qc.logical_usage AS "usage",
qc.advisory_limit AS advisory,
qc.hard_limit AS hard,
qc.inherited AS "default",
qc.timestamp AS "timestamp"

FROM quota_current AS qc

INNER JOIN quota_cluster AS qcl
ON qc.cluster_id =  qcl.id

LEFT JOIN quota_path AS qp
ON qc.path_id =  qp.id

LEFT JOIN quota_type_name AS qtn
ON qc.name_id =  qtn.id

AND qc.type =  qtn.type

WHERE name = "root"
'''

#sql_db_cursor.execute(query)

groups = os.getgroups()

print '/groups/'
for group in groups:
    print '   ' + grp.getgrgid(group).gr_name

print

'''
handle lustre quotas
'''
def get_lustre_quota(group):
    lustre = subprocess.Popen("lfs quota -g " + group + " /hms/scratch1/", shell=True, stdout=subprocess.PIPE).stdout.read()
    lustre = lustre.split('\n')[2].split()
    
    # convert kb to Gb
    lustre_usage = sizeof_fmt(int( float(lustre[1]) * 1024 ))
    lustre_quota = sizeof_fmt(int( float(lustre[3]) * 1024 ))

    group_disp = group[0:20]
    if len(group) < 7:
        group_disp += '\t'
    if len(group) < 14:
        group_disp += '\t'
    if len(group) > 20:
        group_disp += '~'
        
    print '   %s\t%s\t%s' % (group_disp,
                               lustre_usage.rjust(7),
                               lustre_quota.rjust(7))

print '/hms/scratch1/'
for group in groups:
    get_lustre_quota(grp.getgrgid(group).gr_name)
        
