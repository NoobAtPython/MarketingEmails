import pymssql
import datetime as dt
import pandas as pd

# Queries below pull the email lists from database.
EmailQuery1 =  """
                select distinct(kap.ParentEmail) as 'EmailList_1'
                from sso.dbo.profile_table kap with (NOLOCK)
                left join sso.dbo.email_unsubs unsub with (NOLOCK) on unsub.userid = kap.userid
                where ProductGroupID in (1,2)
                AND kap.parentemail is NOT NULL and parentemail like '%@%'
                """

EmailQuery2 = """
                select distinct(kap.ParentEmail) as 'EmailList_2'
                from sso.dbo.profile_table kap with (NOLOCK)
                left join sso.dbo.email_unsubs unsub with (NOLOCK) on unsub.userid = kap.userid
                where ProductGroupID in (9)
                AND kap.parentemail is NOT NULL and parentemail like '%@%'
                """

# Queries for EmailQuery3 and EmailQuery4 require specific product IDs, instead of product group ID.
# Profile table joins with registration table to grab the product the user registered for. 
EmailQuery3 = """
                  select distinct(kap.ParentEmail) as 'EmailList_3'
                  from sso.dbo.profile_table kap with (NOLOCK)
                  inner join sso.dbo.user_registration uia with (NOLOCK) on uia.userid = kap.UserID
                  inner join sso.dbo.applications a with (NOLOCK) on a.applicationid = uia.applicationid 
                  left join sso.dbo.email_unsubs unsub with (NOLOCK) on unsub.userid = kap.userid
                  where productid IN
                  (
                   '252',
                   '253',
                   '258',
                   '261'
                   )
                  AND kap.parentemail is NOT NULL and parentemail like '%@%'
                  """

EmailQuery4 = """            
                    select distinct(kap.ParentEmail) as 'EmailList_4'
                    from sso.dbo.profile_table kap with (NOLOCK)
                    inner join sso.dbo.user_registration uia with (NOLOCK) on uia.userid = kap.UserID
                    inner join sso.dbo.applications a with (NOLOCK) on a.applicationid = uia.applicationid 
                    left join sso.dbo.email_unsubs unsub with (NOLOCK) on unsub.userid = kap.userid
                    where productid IN
                    (
                    '254',
                    '255',
                    '259',
                    '262'
                    )
                    AND kap.parentemail is NOT NULL and parentemail like '%@%'
                """

EmailQuery5 =   """
                select distinct(kap.ParentEmail) as 'EmailList_5'
                from sso.dbo.profile_table kap with (NOLOCK)
                left join sso.dbo.email_unsubs unsub  with (NOLOCK) on unsub.userid = kap.userid
                where ProductGroupID in (15)
                AND kap.parentemail is NOT NULL and parentemail like '%@%' 
                """

# Lists created for filename and email query variables. These are looped through to save each query into its own CSV file.
# Folder path is the location to save CSVs.
fileNames = ['Email_List1', 'Email_List2', 'Email_List3', 'Email_List4', 'Email_List5]
emailQueries = [EmailQuery1, EmailQuery2, EmailQuery3, EmailQuery4, EmailQuery5]
folderPath = "C:/Users/BReyes/Desktop/Bryans_Folder/"

# Today variable made for file name. Connecting to SQL database.
today = dt.datetime.today()
todayparsed = today.strftime('%m %d %Y')
conn = pymssql.connect(server='sql_server.net', user='username', password='secret_password', database ='db')
cursor = conn.cursor()

#Loops through filename and emailQueries lists and then saves each query's results with the appropriate file name into the directory.
for count, query in enumerate(emailQueries):
    for nextcount, file in enumerate(fileNames):
        if count == nextcount:
            df = pd.read_sql(query, conn)
            df.to_csv(folderPath + file +  ' ' + str(todayparsed) + '.csv', index=False)

