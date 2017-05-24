import mysql.connector
from pandas.io import sql
import pandas as pd
import datetime
from gsheet_creds_files.getDataUsingGsheets import get_data
import os
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(root_path)
from config.db_config import credentials
from utility_scripts import mail_utility, upload_to_s3, gmail_utility

cred = credentials['hl_read']
cnx = mysql.connector.connect(user = cred['user'], password = cred['password'], host = cred['host'], database = cred['database'])
offset = 1
script_date = datetime.date.today()  - datetime.timedelta(days = offset)
date2 = script_date - datetime.timedelta(1)

def full_data(cnx,date, cluster_ids):
    query = 'select a.id as OrderId, a.house_number as HouseNumber, a.locality as Locality, a.source as Source, a.accepted_flag as AcceptedBy, a.pickup_flag as PickupBy, a.delivered_flag as DeliveredBy, a.status as Status, convert_tz(scheduled_time, "UTC", "Asia/Kolkata") as OrderTime, convert_tz(pickup_time, "UTC", "Asia/Kolkata") as PickupTime, convert_tz(delivered_time, "UTC", "Asia/Kolkata") as DeliverTime, '
    query += 'convert_tz(allot_time, "UTC", "Asia/Kolkata") as AllotTime, convert_tz(accept_time, "UTC", "Asia/Kolkata") as AcceptTime, convert_tz(message_time, "UTC", "Asia/Kolkata") as MessageTime, amount as Amount, a.latitude as Latitude, a.longitude as Longitude, e.id as SellerID, b.operational_city as City, '
    query += 'c.id as RiderID,f.latitude as SellerLatitude, f.longitude as SellerLongitude, issue as Issue,cancel_reason as cancel_reason, b.cluster_name as Cluster, if(c.id=1,"-",d.first_name )as Rider, e.outlet_name as Seller from coreengine_order as a, coreengine_cluster as b, coreengine_sfxrider as c, '
    query += 'coreengine_riderprofile as d, coreengine_sfxseller as e, coreengine_pickupaddress as f where a.cluster_id = b.id and a.seller_id = e.id and e.address_id = f.id and a.rider_id = c.id and c.rider_id = d.id and '
    query += '(a.status < 10) and date(scheduled_time) = "' + date.strftime("%Y%m%d") + '" and a.source != 9 and b.id in (' + ','.join(str(id) for id in cluster_ids) +')  and outlet_name not like "%reverse%" and outlet_name not like "%test%" and outlet_name not like "%sfx%" and outlet_name not like "%snapdeal%" and outlet_name not like "%DTDC%" and outlet_name not like "%Dummy%";'

    df = sql.read_sql(query, cnx)
    df['DeliverTime'] = pd.to_datetime(df['DeliverTime'])
    df['PickupTime'] = pd.to_datetime(df['PickupTime'])
    df['AllotTime'] = pd.to_datetime(df['AllotTime'])
    df['OrderTime'] = pd.to_datetime(df['OrderTime'])
    df['OrderToDeliver'] = (df['DeliverTime'] - df['OrderTime']).astype('timedelta64[m]')
    df['PickupToDeliver'] = (df['DeliverTime'] - df['PickupTime']).astype('timedelta64[m]')
    df['OrderToAllot'] = (df['AllotTime'] - df['OrderTime']).astype('timedelta64[m]')
    df['OrderHour'] = pd.DatetimeIndex(df['OrderTime']).hour

    return df

def seller_rating(cnx,city, date):
    query = 'select convert_tz(rating_date, "UTC", "Asia/Kolkata") as RatingTime,(rating+1) as rating , comment, b.outlet_name as seller,cluster_name, b.id as SellerID from coreengine_sellerrating as a, coreengine_sfxseller as b, coreengine_cluster as d  '
    query += 'where a.seller_id = b.id and b.cluster_id = d.id and d.id!=1 and date(rating_date) = "' + date.strftime("%Y%m%d") + '" and d.operational_city = "' + city + '" order by cluster_name;'

    try:
        df = sql.read_sql(query,cnx)
        return df
    except Exception, e:
        return pd.DataFrame()


def cancelled_order(cnx,city, date):
    cancel_reason_dict = {
        0: 'Order Cancelled by Consumer',
        1: 'No Rider Assigned',
        2: 'Rider late for pickup',
        3: 'Double Order Punched',
        4: 'No Reason specified',
        5:'Duplicate Order',
        6:'Order Cancelled by Seller',
        7:'Distance for delivery is not serviceable',
        8:'Rider is not available',
        9:'Test Order',
        10:'Take Away',
        11:'Wrong Delievery Address',
        -1: 'Not Reported'
        }

    cancel_reason_df = pd.DataFrame.from_dict(cancel_reason_dict, orient = 'index')
    query = 'select convert_tz(scheduled_time, "UTC", "Asia/Kolkata") as OrderTime, b.outlet_name as Seller, cancel_reason as Reason, b.id as SellerID, a.id as orderID, d.cluster_name, a.rider_id as RiderID, a.allot_time from coreengine_order as a, coreengine_sfxseller as b,'
    query += 'coreengine_cluster as d, coreengine_chain as ch where ch.id=b.chain_id and a.seller_id = b.id and d.id!=1 and a.cluster_id = d.id and date(scheduled_time) = "' + date.strftime("%Y%m%d") + '" and a.status = 302 and a.cancel_reason not in (0,3,5) and chain_name not like "%big%basket%" and a.source !=9 and d.operational_city = "' + city + '" ;'

    query_canceltime  = 'SELECT object_pk as orderID ,convert_tz(action_time, "UTC", "Asia/Kolkata") as CancelledTime from coreengine_auditlog where new_value = 302 and field_name = "status" and table_name = "Order" and date(action_time) = "'+ date.strftime("%Y%m%d") +'"' 
    canceltime_df = sql.read_sql(query_canceltime, cnx)

        
    try:
        cancel_order = sql.read_sql(query, cnx)
        cancel_order = pd.merge(cancel_order, canceltime_df, on='orderID', how='left')
        cancel_order['Reason'] = cancel_order['Reason'].map(cancel_reason_df[0])
        return cancel_order
    except Exception, e:
        return pd.DataFrame()

def cluster_order(df):
    cluster_df = df.groupby('Cluster')['Amount'].count()
    cluster_df.loc['Total'] = cluster_df.sum(axis=0)
    return cluster_df

def seller_order(df):
    vendor_df = df.groupby(['Cluster','Seller','SellerID'])['Amount'].count()
    vendor_df.loc['Total'] = vendor_df.sum(axis=0)
    return vendor_df

def rider_order(df):
    rider_df = df.groupby(['Cluster', 'RiderID','Rider'])['Amount'].count()
    rider_df.loc['Total'] = rider_df.sum(axis=0)
    return rider_df

def seller_hour_count(df):
    seller_hour_df = pd.pivot_table(df, index = ['Cluster', 'Seller'], columns = 'OrderHour', values = 'Amount', aggfunc = 'count', fill_value = 0)
    seller_hour_df['Total'] = seller_hour_df.sum(axis=1)
    seller_hour_df.loc[('All Sellers','Total')] = seller_hour_df.sum(axis=0)
    return seller_hour_df

def cluster_hour_count(df):
    cluster_hour_df = pd.pivot_table(df, index = 'Cluster', columns = 'OrderHour', values = 'Amount', aggfunc = 'count', fill_value = 0)
    cluster_hour_df['Total'] = cluster_hour_df.sum(axis=1)
    cluster_hour_df.loc['Total'] = cluster_hour_df.sum(axis=0)
    return cluster_hour_df

def pickup_deliver_time(df):
    bins_value = [0,15,30,45,60,90,1000]
    bin_df = pd.cut(df['PickupToDeliver'], bins = bins_value)
    order_counts = pd.value_counts(bin_df)
    count_df = pd.DataFrame(order_counts)
    # count_df.loc['Total'] = count_df.sum(axis=0)
    return count_df

def order_deliver_time(df):
    bins_value = [0,30,45,60,90,1000]
    bin_df = pd.cut(df['OrderToDeliver'], bins = bins_value)
    order_counts = pd.value_counts(bin_df)
    count_df = pd.DataFrame(order_counts)
    # count_df.loc['Total'] = count_df.sum(axis=0)
    return count_df
def order_amount(df):
    bins_value = [0,50,100,500,1000,5000]
    bin_df = pd.cut(df['Amount'] , bins = bins_value)
    order_counts = pd.value_counts(bin_df)
    count_df = pd.DataFrame(order_counts)
    # count_df.loc['Total'] = count_df.sum(axis=0)
    return count_df

def get_clusters(cnx):
    query = "select cluster_id from coreengine_sfxoperation so inner join auth_user au on au.id=so.user_id inner join coreengine_sfxoperation_cluster soc on soc.sfxoperation_id=so.id inner join coreengine_cluster c on c.id=soc.cluster_id where role=2;"
    cluster_ids = sql.read_sql(query,cnx)
    cluster_ids = cluster_ids.cluster_id.unique()
    return cluster_ids

def manage_files(city):
    try:
        prev_report = root_path+'/DSR/attachments/'+city + '_DSR_' + date2.strftime("%Y%m%d") + '.xlsx'
        s3reportname = '/DSR/attachments/'+city + '_DSR_' + date2.strftime("%Y%m%d") + '.xlsx'
        uploadstatus = upload_to_s3.upload_to_s3(prev_report,s3reportname,bucket='sfx_reports')
        print uploadstatus
        os.remove(prev_report)
    except IOError:
        print 'IO Error'
        pass

def main():
    cluster_ids = get_clusters(cnx)
    full_df = full_data(cnx,script_date, cluster_ids)
    emails = get_data('DSR')
    df = full_df.copy()
    city_group = df.groupby('City')

    for city, city_df in city_group:
        if city in ['PNQ']:
            continue
        # city_report_name = city
        # if city == "DEL/NOIDA":
        #     city_report_name = "DEL-NOIDA"
        report = root_path+'/DSR/attachments/'+city + '_DSR_' + script_date.strftime("%Y%m%d") + '.xlsx'
        writer = pd.ExcelWriter(report)
        html_text = '<html><body>'

        cluster_df = cluster_order(city_df).reset_index()
        cluster_df.to_excel(writer, 'Cluster Orders', index = True)
        html_text += '<br>' + cluster_df.to_html()

        seller_df = seller_order(city_df).reset_index()
        seller_df.to_excel(writer, 'Seller Orders', index = False)

        rider_df = rider_order(city_df).reset_index()
        rider_df.to_excel(writer, 'Rider Orders', index = False)

        seller_hour_df = seller_hour_count(city_df).reset_index()
        seller_hour_df.to_excel(writer, 'Seller Hourwise', index = False)

        cluster_hour_df = cluster_hour_count(city_df).reset_index()
        cluster_hour_df.to_excel(writer, 'Cluster Hourwise', index = False)

        cancelled_df = cancelled_order(cnx,city,script_date)
        cancelled_df.to_excel(writer, 'Cancelled Orders', index = False)
        html_text += '<br>' + cancelled_df.to_html()

        seller_rating_df = seller_rating(cnx,city,script_date)
        seller_rating_df.to_excel(writer, 'Seller Ratings', index = False)
        html_text += '<br>' + seller_rating_df.to_html()
        
        order_amount_df = order_amount(city_df)
        order_amount_df.to_excel(writer,'Order Amount', index = False)
        html_text += '<br>' + 'Order amount distribution'
        html_text += '<br>' + order_amount_df.to_html()
        
        
        pickup_deliver_time(city_df).to_excel(writer, 'PickupToDeliver')
        order_deliver_time(city_df).to_excel(writer, 'OrderToDeliver')
        writer.save()
        
        recipient_list = emails.get(city, 'sanjay.garg@shadowfax.in')
        cc_list = emails.get('cc', 'sanjay.garg.shadowfax.in')
        bcc_list = []
        # recipient_list = ['sanjay.garg@shadowfax.in']
        # cc_list = ['sanjay.garg@shadowfax.in']
        mail_subject = city + " DSR" + script_date.strftime('%d %b')
        mimetype_dict = {}
        html_text += '</body></html>'
        mimetype_dict['html'] = html_text
        # mailServer = mailUtility.init_mailServer()
        gmail_utility.send_email(recipient_list, cc_list, bcc_list,mail_subject,mimetype_dict,attach=report)
        # mailUtility.send_email(recipient_list, cc_list, bcc_list,mail_subject,mimetype_dict,mailServer,report)
        # mailUtility.close_mailServer(mailServer)
        manage_files(city)

if __name__=='__main__':
    main()
