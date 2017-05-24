import pandas as pd
from pandas.io import sql
import mysql.connector
import math
import os
import datetime
import traceback
from gsheet_creds_files.getDataUsingGsheets import get_data
root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys
sys.path.append(root_path)
from config.db_config import credentials
from utility_scripts import mail_utility, gmail_utility

cred = credentials['hl_read']
cnx = mysql.connector.connect(user = cred['user'], password = cred['password'], host = cred['host'], database = cred['database'])
cred = credentials['ecom_read']
cnx_ecom = mysql.connector.connect(user = cred['user'], password = cred['password'], host = cred['host'], database = cred['database'])
offset = 1
date_today = datetime.date.today() - datetime.timedelta(days = offset)

def getTotalOrders(cnx, cnx_ecom,date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE WHEN ROUND(SUM(pay.total_charge_from_slabs),0) IS NULL THEN 0 ELSE ROUND(SUM(pay.total_charge_from_slabs),0) END as revenue , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord inner join coreengine_cluster clu on ord.cluster_id =  clu.id inner join coreengine_sfxseller as sfx on ord.seller_id = sfx.id inner join coreengine_chain as ch on sfx.chain_id = ch.id LEFT JOIN payments_orderinvoicedata as pay on pay.order_id = ord.id and pay.total_charge_from_slabs <= 1000 " \
            "where (ord.status < 10) and ord.cluster_id !=1 and date(scheduled_time) = '"+ str(date_order) +"' and ord.source != 9 AND outlet_name not like '%reverse%' and outlet_name not like '%test%' and outlet_name not like '%sfx%' and outlet_name not like '%snapdeal%' and outlet_name not like '%DTDC%' and outlet_name not like '%Dummy%' and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name;"
    df = sql.read_sql(query, cnx)
    ecomorders = getHLEcomOrders(cnx,cnx_ecom,date_order,cluster_ids,tm_names)
    ecomorders.rename(columns={'total_orders':'total_orders_ecom'},inplace=True)
    ecomorders['revenue_ecom'] = ecomorders.total_orders_ecom*40
    if ecomorders.shape[0] == 0:
        return df
    else:
        df = (df.merge(ecomorders, on='tm_name', how='left')).fillna(0)
        df['total_orders'] = df.total_orders + df.total_orders_ecom
        df['revenue'] = df.revenue + df.revenue_ecom
        df.drop(['revenue_ecom','total_orders_ecom'],inplace=True,axis=1)
        return df

def get_perorder_expense(cnx,cnx_ecom,date_order,cluster_ids,tm_names,perorder_riders):
    query = "SELECT er.rider_id, count(dr.id)ecom_orders, case "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN dr.cluster_id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "end as tm_name from ecommerce_deliveryrequest dr inner join ecommerce_rider er on er.id=dr.rider_id where date(dr.last_updated)= '" + date_order + "' and order_status=4 and er.rider_id IN (" + ",".join(str(id) for id in perorder_riders) +") group by 1;"
    ecomorders = sql.read_sql(query,cnx_ecom)
    query = "SELECT a.rider_id, count(a.id) as hl_orders, case "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN a.cluster_id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "end as tm_name from coreengine_order a left join coreengine_cluster b on a.cluster_id=b.id "\
            "left join coreengine_sfxrider c on a.rider_id=c.id left join coreengine_riderprofile d on d.id=c.rider_id where date(a.scheduled_time) = '"+str(date_order)+"' and (a.status < 10) and b.id IN (" + ",".join(str(id) for id in cluster_ids) +") and c.payout_type=2 "\
            "and concat(d.first_name,d.last_name) not like '%asket%' and concat(d.first_name,d.last_name) not like '%ummy%' group by 1,3;"
    hlorders = sql.read_sql(query,cnx)
    perorder_expense = hlorders.merge(ecomorders,how='outer',on=['rider_id','tm_name']).fillna(0)
    perorder_expense['orders'] = perorder_expense.ecom_orders + perorder_expense.hl_orders
    perorder_expense['basic_pay'] = perorder_expense.orders * 35
    perorder_expense['incentive'] = 0
    perorder_expense.loc[perorder_expense.orders>=5, 'incentive'] = 75
    perorder_expense.loc[perorder_expense.orders>=10, 'incentive'] = 150
    perorder_expense.loc[perorder_expense.orders>=15, 'incentive'] = 225
    perorder_expense['expense_perorder'] = perorder_expense.basic_pay + perorder_expense.incentive
    perorder_expense.drop(['ecom_orders','hl_orders','basic_pay','incentive','orders'],inplace=True, axis=1)
    perorder_expense = pd.pivot_table(perorder_expense,index='tm_name',values='expense_perorder',aggfunc = sum).reset_index()
    return perorder_expense


def getExpense(cnx, cnx_ecom, date_order, cluster_ids, tm_names):
    # date_yest = (datetime.datetime.strptime(date_order, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    query = "SELECT ROUND((SUM(basic_pay) + SUM(overtime) + sum(incentive) + sum(kilometer)),0) as expense , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query +="END as tm_name from `coreengine_riderdailypayouts` as rp, `coreengine_sfxrider` as rd, coreengine_cluster as clu, coreengine_riderprofile e where payout_date = '"+ date_order +"' and rp.rider_id = rd.id and rd.cluster_id = clu.id and rd.rider_id=e.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and rd.payout_type!=2 and concat(e.first_name,e.last_name) not like '%asket%' and concat(e.first_name,e.last_name) not like '%ummy%' GROUP BY tm_name ;"
    df = sql.read_sql(query, cnx)
    # df_2 = sql.read_sql(query_incentive, cnx)
    query = "SELECT sr.id from coreengine_sfxrider sr where sr.status=1 and sr.payout_type = 2 and sr.cluster_id IN (" + ",".join(str(id) for id in cluster_ids) +");"
    perorder_riders = tuple(sql.read_sql(query,cnx).id.tolist())
    if len(perorder_riders) !=0:
        df_2 = get_perorder_expense(cnx,cnx_ecom,date_order,cluster_ids,tm_names,perorder_riders)
        if len(df_2):
            df = df.merge(df_2, how='outer', on='tm_name')
            df['expense'] = df['expense'].fillna(0) + df['expense_perorder'].fillna(0)
    return df

def getHLEcomOrders(cnx, cnx_ecom,date_order, cluster_ids, tm_names):
    # query = "SELECT sr.id from coreengine_sfxrider sr inner join coreengine_cluster c on c.id=sr.cluster_id where cluster_name like '%hub%' and cluster_name not like '%test%' and sr.status=1 and c.id IN (" + ",".join(str(id) for id in cluster_ids) +");"
    # ecomriders = tuple(sql.read_sql(query,cnx).id.tolist())
    query = "SELECT COUNT(dr.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN cluster_id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from ecommerce_deliveryrequest dr inner join ecommerce_rider er on er.id=dr.rider_id where date(dr.last_updated)= '" + date_order + "' and order_status=4 and cluster_id IN (" + ",".join(str(id) for id in cluster_ids) +") group by tm_name;"
    df = sql.read_sql(query, cnx_ecom)
    HLReverse = getHLReverseOrders(cnx,date_order,cluster_ids, tm_names)
    if HLReverse.shape[0]!=0:
        df = df.merge(HLReverse,on='tm_name',how='outer')
        df = df.fillna(0)
        df.total_orders = df.total_orders + df.hlreverse_orders
        df.drop('hlreverse_orders',inplace=True,axis=1)
    return df

def getHLReverseOrders(cnx, date_order, cluster_ids, tm_names):
    query = "select count(*)hlreverse_orders, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN o.cluster_id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order o inner join coreengine_sfxseller sf on o.seller_id=sf.id where date(scheduled_time) = '" + date_order + "' and sf.outlet_name like '%reverse%' and outlet_name not like '%hub%' and o.cluster_id IN (" + ",".join(str(id) for id in cluster_ids) +") group by 2;"
    df = sql.read_sql(query, cnx)
    return df

# def getHLEcomOrders(cnx, cnx_ecom,date_order, cluster_ids, tm_names):
#     query = "SELECT COUNT(ord.id) as total_orders , CASE "
#     for tm_name in tm_names:
#         cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
#         query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
#     query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_sfxseller as sfx, coreengine_chain as ch where (ord.status < 6 or ord.status=8) and ord.cluster_id !=1 and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and ord.seller_id = sfx.id and sfx.chain_id = ch.id and ((ord.source = 9 and clu.cluster_name NOT LIKE '%hub%')) and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name"
#     df = sql.read_sql(query, cnx)
#     return df

def getHLOrders(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_sfxseller as sfx, coreengine_chain as ch where (ord.status < 10) and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and ord.seller_id = sfx.id and sfx.chain_id = ch.id and ( ord.source != 9) and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and outlet_name not like '%reverse%' and outlet_name not like '%test%' and outlet_name not like '%sfx%' and outlet_name not like '%snapdeal%' and outlet_name not like '%DTDC%' and outlet_name not like '%Dummy%' GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getCancelledOrders(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_sfxseller as sf, coreengine_chain as ch where ch.id=sf.chain_id and ord.seller_id = sf.id and ord.status in (302) and chain_name not like '%big%basket%' and ord.cancel_reason not in (0,3,5) and ord.source !=9 and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getAppOrders(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu , coreengine_sfxseller as sfx, coreengine_chain as ch where ord.accepted_flag = 1 and ord.source!=2 and ord.pickup_flag = 1 and ord.delivered_flag = 1 and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and (ord.status < 10) and ord.seller_id = sfx.id and sfx.chain_id = ch.id and ord.source != 9 and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and outlet_name not like '%reverse%' and outlet_name not like '%test%' and outlet_name not like '%sfx%' and outlet_name not like '%snapdeal%' and outlet_name not like '%DTDC%' and outlet_name not like '%Dummy%' GROUP BY tm_name"
    df_app = sql.read_sql(query, cnx)

    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_sfxseller as sfx where (ord.status < 10) and ord.source!=2 and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and ord.seller_id = sfx.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and ord.source != 9 and outlet_name not like '%reverse%' and outlet_name not like '%test%' and outlet_name not like '%sfx%' and outlet_name not like '%snapdeal%' and outlet_name not like '%DTDC%' and outlet_name not like '%Dummy%' GROUP BY tm_name"
    df_total_app = sql.read_sql(query, cnx)
    return df_app, df_total_app

def getFromSellersOrders(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_sfxseller as sfx, coreengine_chain as ch where ord.source not in (1,2) and (ord.status < 10) and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and ord.seller_id = sfx.id and sfx.chain_id = ch.id and ord.source != 9 and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and outlet_name not like '%reverse%' and outlet_name not like '%test%' and outlet_name not like '%sfx%' and outlet_name not like '%snapdeal%' and outlet_name not like '%DTDC%' and outlet_name not like '%Dummy%' GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getTotalRiderEquivalent(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT count(att.id) as total_riders, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name, role from `coreengine_riderattendance` as att, `coreengine_sfxrider` as sfx, coreengine_cluster as clu, `coreengine_riderprofile` as rid where attendancedate = '"+ date_order + "' and att.cluster_id = clu.id and att.rider_id = sfx.id and sfx.status=1 and sfx.rider_id = rid.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and concat(rid.first_name,rid.last_name) not like '%ummy%' and concat(rid.first_name,rid.last_name) not like '%asket%' GROUP BY tm_name, rid.role"
    df = sql.read_sql(query, cnx)
    return df

def getPresentRiderEquivalent(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT count(att.id) as total_riders, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name, role from `coreengine_riderattendance` as att, `coreengine_sfxrider` as sfx, coreengine_cluster as clu, `coreengine_riderprofile` as rid where attendancedate = '"+ date_order + "' and att.cluster_id = clu.id and att.rider_id = sfx.id and sfx.rider_id = rid.id and att.status in (0) and sfx.status=1 and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and concat(rid.first_name,rid.last_name) not like '%ummy%' and concat(rid.first_name,rid.last_name) not like '%asket%' GROUP BY tm_name, rid.role"
    df = sql.read_sql(query, cnx)
    return df

def getNoRecordRiderFTE(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT count(att.id) as total_riders, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name, role from `coreengine_riderattendance` as att, `coreengine_sfxrider` as sfx, coreengine_cluster as clu, `coreengine_riderprofile` as rid where attendancedate = '"+ date_order  + "' and att.cluster_id = clu.id and att.rider_id = sfx.id and sfx.rider_id = rid.id and att.status = -1 and sfx.status=1 and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and concat(rid.first_name,rid.last_name) not like '%ummy%' and concat(rid.first_name,rid.last_name) not like '%asket%' GROUP BY tm_name, rid.role"
    df = sql.read_sql(query, cnx)
    return df


def getRidersOFF(cnx, date_order, statuses, cluster_ids, tm_names):
    query = "SELECT count(att.id) as total_riders, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name, role from `coreengine_riderattendance` as att, `coreengine_sfxrider` as sfx, coreengine_cluster as clu, `coreengine_riderprofile` as rid where attendancedate = '"+ date_order  + "' and att.cluster_id = clu.id and att.rider_id = sfx.id and sfx.rider_id = rid.id and sfx.status=1 and att.status in (" + ",".join(str(st) for st in statuses) +") and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and concat(rid.first_name,rid.last_name) not like '%ummy%' and concat(rid.first_name,rid.last_name) not like '%asket%' GROUP BY tm_name, rid.role"
    df = sql.read_sql(query, cnx)
    return df

def getActiveSellers(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT count(DISTINCT(ord.seller_id)) as total_sellers , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu where date(scheduled_time) = '" + date_order + "' and ord.cluster_id = clu.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getFnBOrders(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT COUNT(ord.id) as total_orders , CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord , coreengine_cluster as clu, coreengine_chain as ch,  coreengine_sfxseller as sfx, coreengine_chainbenchmark as cb where (ord.status < 10) and date(scheduled_time) = '"+ date_order +"' and ord.cluster_id =  clu.id and ord.seller_id = sfx.id and sfx.chain_id = ch.id and ch.merchant_type_id = cb.id and cb.merchant_type = 1 and ord.source != 9 and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getOrdersEfficiency(cnx, date_time_order_start, date_time_order_end, cluster_ids, tm_names):
    query = "SELECT count(ord.id) as total_orders, CASE " 
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name from coreengine_order as ord, coreengine_cluster as clu where (ord.status < 10) and scheduled_time >= '"+ date_time_order_start.strftime("%Y-%m-%d %H:%M:%S") +"' and scheduled_time <= '"+ date_time_order_end.strftime("%Y-%m-%d %H:%M:%S") +"' and ord.cluster_id =  clu.id and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") and ((ord.source = 9 and clu.cluster_name NOT LIKE '%hub%') OR ord.source != 9) GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getOverTime(cnx, date_order, cluster_ids, tm_names):
    query = "SELECT CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name,SUM(case WHEN rid.role = 'PRT' THEN (TIMESTAMPDIFF(HOUR, actual_intime, actual_outtime) - 4) ELSE (TIMESTAMPDIFF(HOUR, actual_intime, actual_outtime)-9) END) as total_hours from coreengine_riderattendance as r,coreengine_cluster as clu, coreengine_sfxrider as sfx, coreengine_riderprofile as rid WHERE attendancedate ='"+ date_order +"' and r.rider_id = sfx.id and sfx.rider_id = rid.id and r.cluster_id = clu.id and sfx.status=1 and r.status = 0 and ((rid.role = 'FT' and TIMESTAMPDIFF(HOUR, actual_intime, actual_outtime) > 9) OR (rid.role = 'PRT' and TIMESTAMPDIFF(HOUR, actual_intime, actual_outtime) > 4)) and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +") GROUP BY tm_name"
    df = sql.read_sql(query, cnx)
    return df

def getRidersSlotTime(cnx, date_today, cluster_ids, tm_names):
    query = "SELECT clu.operational_city as city, CASE "
    for tm_name in tm_names:
        cluster_ids_tm = '(' + ",".join(str(id) for id in tm_names[tm_name]) +')'
        query += "WHEN clu.id IN "+ cluster_ids_tm + " THEN '" + tm_name +"' "
    query += "END as tm_name , rad.actual_intime as actual_intime, rad.actual_outtime as actual_outtime, rp.role as role, sfx.id as sfxid from coreengine_riderattendance as rad, coreengine_sfxrider as sfx, coreengine_riderprofile as rp, coreengine_cluster as clu where rad.status = 0 and rad.rider_id = sfx.id and sfx.rider_id = rp.id and sfx.cluster_id = clu.id and sfx.status = 1 and attendancedate = '"+ date_today+"' and clu.id IN (" + ",".join(str(id) for id in cluster_ids) +")";
    df = sql.read_sql(query, cnx)
    return df

def calculateOvertimeEfficiency(eff_data, total_riders, data_key ,key_total='total_orders'):
    data_percent = {'Total':0}
    for key in data_key:
        data_percent[key] = 0
    for id, data in eff_data.iterrows():
        try:
            data_percent[data['tm_name']]=float(round(float(float(data[key_total])/float(total_riders[data['tm_name']])), 1))
        except ZeroDivisionError:
            data_percent[data['tm_name']] = 0

        data_percent['Total'] += float(data[key_total])
    try:
        data_percent['Total'] = float(round(float(float(data_percent['Total'])/float(total_riders['Total'])), 1))
    except ZeroDivisionError:
        data_percent['Total'] = 0
    return data_percent

def calculateOrderEfficiency(eff_data, total_riders_manhour,eff_type, data_key):
    data_percent = {'Total':0}
    for key in data_key:
        data_percent[key] = 0
    for id, data in eff_data.iterrows():
        try:
            data_percent[data['tm_name']]=float(round(float(float(data['total_orders'])/float(total_riders_manhour[data['tm_name']][eff_type])), 1))
        except ZeroDivisionError:
            data_percent[data['tm_name']] = 0
        data_percent['Total'] += float(data['total_orders'])
    try:
        data_percent['Total'] = float(round(float(float(data_percent['Total'])/float(total_riders_manhour['Total'][eff_type])), 1))
    except ZeroDivisionError:
        data_percent['Total'] = 0
    return data_percent

def calculateSlotManhour(slotdf, dict_key):
    man_hour_dict = {"Total":{"pre_lunch_time_eff":0,"lunch_time_eff":0,"non_peak_eff":0,"dinner_time_eff":0}}
    for key in dict_key:
        man_hour_dict[key] = {"pre_lunch_time_eff":0,"lunch_time_eff":0,"non_peak_eff":0,"dinner_time_eff":0}
    

    for index, rider_df in slotdf.iterrows():
        starting_work_hour = 0
        end_work_hour = 0
        role_map_hour = {"PRT":4,"FT":9}
        in_time = rider_df['actual_intime']
        out_time = rider_df['actual_outtime']
        tm_name = rider_df['tm_name']
        try:
            total_work_hours = role_map_hour[rider_df['role']]
        except:
            total_work_hours = 9
        if pd.isnull(in_time) == True:
            try:
                if pd.isnull(out_time) == True:
                    continue
            except TypeError:
                print traceback.format_exc()
                if pd.isnull(out_time) == True:
                    continue
            else:
                out_time += datetime.timedelta(hours=5, minutes = 30) 
                starting_work_hour = int(out_time.hour) - total_work_hours
                end_work_hour = int(out_time.hour)
        else:
            in_time += datetime.timedelta(hours=5, minutes = 30)
            starting_work_hour = int(in_time.hour) 
            end_work_hour = int(in_time.hour) + total_work_hours
        for i in range(starting_work_hour, end_work_hour):
            if i >=8 and i < 11:
                man_hour_dict[tm_name]['pre_lunch_time_eff']+=1
                man_hour_dict['Total']['pre_lunch_time_eff']+=1
            elif i >=11 and i < 15:
                man_hour_dict[tm_name]['lunch_time_eff']+=1
                man_hour_dict['Total']['lunch_time_eff']+=1
            elif i>=15 and i<19:
                man_hour_dict[tm_name]['non_peak_eff']+=1
                man_hour_dict['Total']['non_peak_eff']+=1
            else:
                man_hour_dict[tm_name]['dinner_time_eff']+=1
                man_hour_dict['Total']['dinner_time_eff']+=1
    return man_hour_dict


    

def calculate_ordersdata(total_orders, data_key, key='total_orders'):
    data_ret = {'Total':0}
    for key_ in data_key:
        data_ret[key_] = 0
    for id, data in total_orders.iterrows():
        data_ret[data['tm_name']] += int(data[key]) 
        data_ret['Total'] += int(data[key])
    return data_ret

def calculate_recovery(revenue, expense, data_key):
    data_percent = {'Total':0}
    for key in data_key:
        data_percent[key] = 0
    for key in revenue:
        try:
            data_percent[key] =  str(int(round(float(float(revenue[key])/float(expense[key]))*100, 0)))+"%"
        except ZeroDivisionError:
            data_percent[key] = '0%'
    return data_percent 

def calculate_orderspercent(order_data, total_orders_data, data_key):
    data_percent = {'Total':0}
    for key in data_key:
        data_percent[key] = 0
    for id, data in order_data.iterrows():
        try:
            data_percent[data['tm_name']]=str(int(round(float(float(data['total_orders'])/float(total_orders_data[data['tm_name']]))*100, 0)))+"%"
        except ZeroDivisionError:
            data_percent[data['tm_name']]= '0%'
        data_percent['Total'] += int(data['total_orders'])
    try:
        data_percent['Total'] = str(int(round(float(float(data_percent['Total'])/float(total_orders_data['Total']))*100, 0))) + "%"
    except ZeroDivisionError:
        data_percent['Total'] = '0%'
    return data_percent


def calculate_sellers(sellers, data_key):
    data_sellers ={"Total":0}
    for key in data_key:
        data_sellers[key] = 0
    for id, data in sellers.iterrows():
        data_sellers[data['tm_name']] += int(data['total_sellers'])
        data_sellers['Total'] += int(data['total_sellers'])
    return data_sellers

def calculate_riders(riders, data_key):
    data_riders = {"Total":0}
    for key in data_key:
        data_riders[key] = 0
    for id, data in riders.iterrows():
        if data['role'] == 'PRT':
            data_riders[data['tm_name']] += float(data['total_riders'])/2
        elif data['role'] == 'FT':
            data_riders[data['tm_name']] += float(data['total_riders'])
    
    clusters = data_riders.keys()
    for cluster in clusters:
        if cluster != 'Total':
            data_riders['Total'] += data_riders[cluster]
    return data_riders

def calculate_riderspercent(data_riders, total_riders_data, data_key):
    data_riders_percent = {"Total":0}
    for key in data_key:
        data_riders_percent[key] = 0
    for id, data in data_riders.iterrows():
        if data['role'] == 'PRT':
            data_riders_percent[data['tm_name']] += int(data['total_riders'])/2
        elif data['role'] == 'FT':
            data_riders_percent[data['tm_name']] += int(data['total_riders'])
    clusters = data_riders_percent.keys()
    for cluster in clusters:
        if cluster != 'Total':
            data_riders_percent['Total'] += data_riders_percent[cluster]
    for cluster in data_riders_percent:
        if cluster != 'Total':
            try:
                data_riders_percent[cluster] = str(int(round(float(float(data_riders_percent[cluster])/float(total_riders_data[cluster]))*100, 0)))+"%"
            except ZeroDivisionError:
                data_riders_percent[cluster] = '0%'

    try:
        data_riders_percent['Total'] = str(int(round(float(float(data_riders_percent['Total'])/float(total_riders_data['Total']))*100, 0))) + "%"
    except ZeroDivisionError:
        data_riders_percent['Total'] = '0%'
    return data_riders_percent 

def build_html(metrics_dict, man_hour_dict,data_key):
    columns = ['Metrics','','Total']
    columns += data_key
    rows = ['Orders','Riders','Efficiency','Sales']
    rows_columns = {'Orders':['Total','Non Ecom','Ecom','Cancelled','App %','Seller portal %', 'F&B %'],'Riders':['#FTE','Present','LWA','Weekly off','Leaves','Att_not_recorded'], 'Efficiency':['Overall','Pre-Lunch','Lunch','Non - Peak','Dinner','overtime per FTE','' ,'Revenue/Order','Cost/Order','Revenue','Expense', '% Recovery'], 'Sales':['# Active Sellers']}
    dict_overall_eff = {'Total':0}
    metrics_dict['Efficiency']['Cost/Order'] = {}
    metrics_dict['Efficiency']['Revenue/Order'] = {} 
    for key in data_key:
        dict_overall_eff[key] = 0
    # for key in dict_overall_eff:
    #     for ky in metrics_dict['Efficiency']:
    #         dict_overall_eff[key] += metrics_dict['Efficiency'][ky][key]
    for key in dict_overall_eff:
        # total_man_hour = sum(man_hour_dict[key].values())
        total_orders = metrics_dict['Orders']['Total'][key]
        total_man_hour=metrics_dict['Riders']['Present'][key]*(9+metrics_dict['Efficiency']['overtime per FTE'][key])
        try:
            dict_overall_eff[key] = float(round(float(float(total_orders)/float(total_man_hour)), 2))
        except ZeroDivisionError:
            dict_overall_eff[key] = 0
        try:
            metrics_dict['Efficiency']['Cost/Order'][key] = float(round(float(float( metrics_dict['Efficiency']['Expense'][key])/float(total_orders)), 1))
        except ZeroDivisionError:
            metrics_dict['Efficiency']['Cost/Order'][key] = 0

        try:
            metrics_dict['Efficiency']['Revenue/Order'][key] = float(round(float(float( metrics_dict['Efficiency']['Revenue'][key])/float(total_orders)), 1))
        except ZeroDivisionError:
            metrics_dict['Efficiency']['Revenue/Order'][key] = 0

    metrics_dict['Efficiency']['Overall'] = dict_overall_eff
    rc_stats = ['Total']
    rc_stats += data_key
    html = '<table cellspacing="0" cellpadding="0" dir="ltr" border="1" style="table-layout:fixed;font-size:13px;font-family:arial,sans,sans-serif;border-collapse:collapse;border:1px solid rgb(204,204,204)"><colgroup><col width="100"><col width="130"><col width="100"><col width="100"><col width="100"><col width="100"></colgroup><tbody><tr style="height:21px">'
    for col in columns:
        html += '<td style="padding:2px 3px;border:1px solid rgb(0,0,0);font-family:arial;font-weight:bold;vertical-align:bottom;background-color:rgb(201,218,248);text-align:center!important">'+ col +'</td>'
    html += '</tr>'
    try:
        for row in rows:
            html += '<tr style="height:21px"> <td style="padding:2px 3px;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0);border-left-width:1px;border-left-style:solid;border-left-color:rgb(0,0,0);font-family:arial;font-weight:bold;vertical-align:bottom;background-color:rgb(201,218,248)">'+ str(row) +'</td>'  
            for id, rc in enumerate(rows_columns[row]):
                if id != 0:
                    html += '<tr style="height:21px"><td style="padding:2px 3px;vertical-align:bottom;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0);border-left-width:1px;border-left-style:solid;border-left-color:rgb(0,0,0);background-color:rgb(201,218,248)"></td>'
                html += '<td style="padding:2px 3px;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0);font-family:arial;font-weight:bold;vertical-align:bottom;background-color:rgb(201,218,248)">'+ str(rc) +'</td>'
                for stat in rc_stats:
                    try:
                        value = metrics_dict[row][rc].get(stat,'')
                    except KeyError:
                        value = ''
                    html += '<td style="padding:2px 3px;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0);font-family:arial;vertical-align:bottom;text-align:center;'
                    if (rc == 'Total' and row == 'Orders') or (rc == '#FTE' and row == 'Riders') or (rc == 'Present' and row == 'Riders') or (row == 'Sales'):
                        html += 'background-color:rgb(201,218,248)'
                    if (rc in ['Overall', 'Cost/Order','Revenue/Order'] and row =='Efficiency'):
                        html += 'background-color:rgb(255,255,0)'
                    html += '">'+str(value) +'</td>'
                html += '</tr>'
            html += '<tr style="height:21px">'
            for i in range (2):
                html += '<td style="padding:2px 3px;vertical-align:bottom;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0);border-left-width:1px;border-left-style:solid;border-left-color:rgb(0,0,0);background-color:rgb(201,218,248)"></td>'
            len_col = len(dict_overall_eff)
            for i in range(len_col):
                html += '<td style="padding:2px 3px;vertical-align:bottom;border-right-width:1px;border-right-style:solid;border-right-color:rgb(0,0,0);border-bottom-width:1px;border-bottom-style:solid;border-bottom-color:rgb(0,0,0)">'
            html += '</tr>'
        html += '</tbody></table>'
    except:
        print traceback.format_exc()
    return html

def send_email(html, date_today,to_addr, city):
    data = get_data('OrderReportSummary_city')
    city_head_map = data
    mailSubject = 'City -'+city+' [Snapshot] Overall DSR '+ date_today.strftime('%d %b')
    mimetype_parts_dict = {'html':html}
    # mailServer = mailUtility.init_mailServer()
    # mailUtility.send_email(['sanjay.garg@shadowfax.in'], [], [], mailSubject, mimetype_parts_dict,mailServer,from_address ='DSRSnapshotsReports@shadowfax.in')
    gmail_utility.send_email(to_addr+city_head_map.get(city,'rohit.jain@shadowfax.in'), city_head_map.get('cc'), [], mailSubject, mimetype_parts_dict)
    # gmail_utility.send_email(['sanjay.garg@shadowfax.in'], [], [], mailSubject, mimetype_parts_dict)
    # try:
    #     mailUtility.send_email(to_addr+city_head_map.get(city,'rohit.jain@shadowfax.in'), city_head_map.get('cc'), [], mailSubject, mimetype_parts_dict,mailServer,from_address ='DSRSnapshotsReports@shadowfax.in',reply_to='management@shadowfax.in')
    # except Exception as e:
    #     pass
    #     i=3
    #     flag =0
    #     while(i):
    #         i-=1
    #         mailServer = mailUtility.close_mailServer(mailServer)
    #         time.sleep(325)
    #         mailServer = mailUtility.init_mailServer()
    #         if (mailUtility.test_conn_open(mailServer)):
    #             try:
    #                 mailUtility.send_email(to_addr+city_head_map.get(city,'rohit.jain@shadowfax.in'), city_head_map.get('cc'), [], mailSubject, mimetype_parts_dict,mailServer,from_address ='DSRSnapshotsReports@shadowfax.in',reply_to='management@shadowfax.in')
    #                 flag =1
    #                 break
    #             except:
    #                 pass
    #     if flag==0:
    #         print 'DBA Snapshots TM- CRON Failure'
    #         print e

def get_tmmapping(cnx):
    query = "select c.operational_city city,au.username,cluster_id from coreengine_sfxoperation so inner join auth_user au on au.id=so.user_id inner join coreengine_sfxoperation_cluster soc on soc.sfxoperation_id=so.id inner join coreengine_cluster c on c.id=soc.cluster_id where role=2 and au.username not like '%test%';"
    df = sql.read_sql(query,cnx)
    pivot = pd.pivot_table(df,index='cluster_id',values='city',aggfunc=len).reset_index()
    cluster_atl = pivot[pivot.city == 2].cluster_id.tolist()
    df['check'] = 0
    df.loc[(df.cluster_id.isin(cluster_atl) & df.username.isin(['harish.naidu','nirupama.das','zia.shaikh','n.srikanth'])), 'check'] = 1
    df = df[df.check == 0]
    df.index = range(len(df))
    tm_map = {}
    for city in df.city.unique():
        tm_map[str(city)] = {}
        for tm in df[df.city == city].username.unique():
            tm_map[str(city)][tm] = []
            for cluster in df[(df.city == city) & (df.username == tm)].cluster_id.unique():
                tm_map[str(city)][tm].append(cluster)
    return tm_map

def main():
    print 'starting process for ', date_today
    tm_map = get_tmmapping(cnx)
    for city_tm in tm_map:
        metrics_dict = {"Orders":{'Total':{},'Ecom':{},'Non Ecom':{},'Cancelled':{},'App %':{},'Seller portal %':{},'F&B %':{}},"Riders":{"#FTE":{},"Present":{},"Present":{},"LWA":{},"Weekly off":{},"Leaves":{},"Att_not_recorded":{}},"Efficiency":{"Pre-Lunch":{},"Lunch":{},"Non - Peak":{},"Dinner":{},"overtime per FTE":{}, "Revenue":{}, 'Expense':{}, '% Recovery':{}},"Sales":{"# Active Sellers":{}}}
        cluster_ids_tm = [c_id for name in tm_map[city_tm] for c_id in tm_map[city_tm][name]]
        tm_names = tm_map[city_tm]

        total_orders = getTotalOrders(cnx, cnx_ecom,date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        ecom_orders = getHLEcomOrders(cnx, cnx_ecom,date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        non_ecom_orders = getHLOrders(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        expense = getExpense(cnx, cnx_ecom,date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        cancelled_orders = getCancelledOrders(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        app_orders , total_app_orders = getAppOrders(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        seller_orders = getFromSellersOrders(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        sellers  = getActiveSellers(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        FnB_orders = getFnBOrders(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        riders_fte = getTotalRiderEquivalent(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        present_riders_fte = getPresentRiderEquivalent(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        norecord_riders_fte = getNoRecordRiderFTE(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        off_riders_fte = getRidersOFF(cnx, date_today.strftime("%Y-%m-%d"), [5], cluster_ids_tm, tm_names)
        lwa_riders_fte = getRidersOFF(cnx, date_today.strftime("%Y-%m-%d"), [2,4], cluster_ids_tm, tm_names)
        leave_riders_fte = getRidersOFF(cnx, date_today.strftime("%Y-%m-%d"), [1, 3], cluster_ids_tm, tm_names)
        pre_lunch_time_eff = getOrdersEfficiency(cnx, datetime.datetime.combine(date_today ,datetime.datetime.strptime("02:30:00", "%H:%M:%S").time()),datetime.datetime.combine(date_today ,datetime.datetime.strptime("05:30:00", "%H:%M:%S").time()), cluster_ids_tm, tm_names)
        lunch_time_eff = getOrdersEfficiency(cnx, datetime.datetime.combine(date_today ,datetime.datetime.strptime("05:30:00", "%H:%M:%S").time()),datetime.datetime.combine(date_today ,datetime.datetime.strptime("09:30:00", "%H:%M:%S").time()), cluster_ids_tm, tm_names)
        non_peak_eff = getOrdersEfficiency(cnx, datetime.datetime.combine(date_today ,datetime.datetime.strptime("09:30:00", "%H:%M:%S").time()),datetime.datetime.combine(date_today ,datetime.datetime.strptime("13:30:00", "%H:%M:%S").time()), cluster_ids_tm, tm_names)
        dinner_time_eff = getOrdersEfficiency(cnx, datetime.datetime.combine(date_today ,datetime.datetime.strptime("13:30:00", "%H:%M:%S").time()),datetime.datetime.combine(date_today ,datetime.datetime.strptime("20:30:00", "%H:%M:%S").time()), cluster_ids_tm, tm_names)
        overtime_eff = getOverTime(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)
        riders_slotdf = getRidersSlotTime(cnx, date_today.strftime("%Y-%m-%d"), cluster_ids_tm, tm_names)         
        man_hour_dict = calculateSlotManhour(riders_slotdf, tm_names)

        metrics_dict['Orders']['Total'] = calculate_ordersdata(total_orders, tm_names)
        metrics_dict['Orders']['Ecom'] = calculate_ordersdata(ecom_orders, tm_names)
        metrics_dict['Orders']['Non Ecom'] = calculate_ordersdata(non_ecom_orders, tm_names)
        metrics_dict['Efficiency']['Revenue'] = calculate_ordersdata(total_orders, tm_names , key = 'revenue')
        metrics_dict['Efficiency']['Expense'] = calculate_ordersdata(expense, tm_names , key = 'expense')
        metrics_dict['Efficiency']['% Recovery'] = calculate_recovery(metrics_dict['Efficiency']['Revenue'], metrics_dict['Efficiency']['Expense'], tm_names)
        total_app = calculate_ordersdata(total_app_orders, tm_names)
        metrics_dict['Orders']['Cancelled'] = calculate_ordersdata(cancelled_orders, tm_names)
        metrics_dict['Orders']['App %'] = calculate_orderspercent(app_orders, total_app,  tm_names)
        metrics_dict['Orders']['Seller portal %'] = calculate_orderspercent(seller_orders, metrics_dict['Orders']['Non Ecom'], tm_names)
        metrics_dict['Orders']['F&B %'] = calculate_orderspercent(FnB_orders, metrics_dict['Orders']['Non Ecom'], tm_names)
        metrics_dict['Sales']['# Active Sellers'] = calculate_sellers(sellers, tm_names)
        metrics_dict['Riders']['#FTE'] = calculate_riders(riders_fte, tm_names)
        metrics_dict['Riders']['Present'] = calculate_riders(present_riders_fte, tm_names)
        metrics_dict['Riders']['LWA'] = calculate_riderspercent(lwa_riders_fte, metrics_dict['Riders']['#FTE'], tm_names)
        metrics_dict['Riders']['Weekly off'] = calculate_riderspercent(off_riders_fte, metrics_dict['Riders']['#FTE'], tm_names)
        metrics_dict['Riders']['Leaves'] = calculate_riderspercent(leave_riders_fte, metrics_dict['Riders']['#FTE'], tm_names)
        metrics_dict['Riders']['Att_not_recorded'] = calculate_riderspercent(norecord_riders_fte, metrics_dict['Riders']['#FTE'], tm_names)
        metrics_dict['Efficiency']['Pre-Lunch'] = calculateOrderEfficiency(pre_lunch_time_eff, man_hour_dict, 'pre_lunch_time_eff', tm_names)
        metrics_dict['Efficiency']['Lunch'] = calculateOrderEfficiency(lunch_time_eff, man_hour_dict, 'lunch_time_eff', tm_names)
        metrics_dict['Efficiency']['Non - Peak'] = calculateOrderEfficiency(non_peak_eff, man_hour_dict, 'non_peak_eff', tm_names)
        metrics_dict['Efficiency']['Dinner'] = calculateOrderEfficiency(dinner_time_eff, man_hour_dict, 'dinner_time_eff', tm_names)
        metrics_dict['Efficiency']['overtime per FTE'] = calculateOvertimeEfficiency(overtime_eff, metrics_dict['Riders']['Present'] , tm_names, key_total = 'total_hours')
        print metrics_dict
        html_template = build_html(metrics_dict, man_hour_dict, tm_names)
        to_addr = [name.split('/')[0] +'@shadowfax.in' if '/' in name else name +'@shadowfax.in' for name in tm_map[city_tm]]
        send_email(html_template, date_today, to_addr, city_tm)
        
if __name__ == "__main__":
    main()
