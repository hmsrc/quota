#!/usr/bin/python

'''
quota.py
5/26/14
'''



import os, sys
import pwd, grp

import ConfigParser as parser
import MySQLdb as sql
import subprocess



def sizeof_fmt(num):
    for x in [' b','Kb','Mb','Gb']:
        if num < 1024.0:
            return '%3.1f%s' % (num, x)
        num /= 1024.0
    return '%3.1f%s' % (num, 'Tb')



def print_quota(cluster, fs_path, ids, cursor):
    print fs_path
    
    for id in ids:
        if cluster == 'itisimdcp03':
            id_disp = pwd.getpwuid(id).pw_name # get username from uid
        else:
            id_disp = grp.getgrgid(id).gr_name # get groupname from gid

        query_base = '''
        SELECT
        qp.path AS path,
        qcl.name AS cluster,
        qc.type AS type,
        qtn.id AS id,
        qtn.name AS name,
        qc.logical_usage AS "usage",
        qc.physical_usage AS "limit",
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
        '''
        
        query_id = 'WHERE (qtn.id = ' + str(id) + ' AND qcl.name = ' + '"' + cluster + '")'

        query = query_base + query_id
        
        cursor.execute(query)

        data = cursor.fetchone()

        if data:
#            print data
            
            if data[5]: # this is logical_usage, i.e. usage
                data_usage = sizeof_fmt(data[5])
            else:
                data_usage = "NA"

            data_quota = "NA"
            
            if cluster == 'itisimdcp05':
                if data[6]: # this is physical_usage, i.e. limit
                    data_quota = sizeof_fmt(data[6])
            else:
                if data[8]: # this is the hard_limit, i.e. hard
                    data_quota = sizeof_fmt(data[8])
            
            print '   %s\t%s\t%s' % (id_disp[0:20].ljust(21), data_usage.rjust(10), data_quota.rjust(10))

    print
        
    return



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
    
    return

def print_lustre_quota(fs_path, groups):
    print fs_path
    
    for group in groups:
        get_lustre_quota(grp.getgrgid(group).gr_name)
    
    return



def main():
    uid = [os.geteuid()]
    
    if len(sys.argv) == 2:
        uid = [pwd.getpwnam(sys.argv[1]).pw_uid]
        os.seteuid(uid[0])
    
    print "quota report".ljust(24) + '\t' + 'Used'.rjust(10) + '\t' + 'Limit'.rjust(10) + '\n'
    
    # get user group membership
    groups = os.getgroups()
    
    # setup the MySQL credential parser
    db_keys = 'my.cnf'
    config = parser.ConfigParser()
    config.read(db_keys)
    
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
    
    # run queries here against p03 and p05
    print_quota('itisimdcp03', '/home/', uid, sql_db_cursor) # p03 or /home/
    print_quota('itisimdcp05', '/groups/', groups, sql_db_cursor) # p05 or /groups/
    
    # lustre fs query
    #print_lustre_quota('/hms/scratch1/', groups) # no p-designation
    
    # close out db connections
    sql_db_cursor.close()
    
    sql_db_conn.close()
    
    return
    
if __name__ == "__main__":
    main()
