# Import libraries
import os
import datetime
import pandas as pd
import openpyxl
from util import rs_conn

def create_pivot_tables(sql_query):
    # Get the redshift user name and password
    rs_user = os.getenv('RS_USER')
    rs_pass = os.getenv('RS_PASS')

    # Connect to the Redshift
    rs = rs_conn.RS_CONN(rs_user=rs_user, rs_pass=rs_pass)
    rs.set_Conn()

    # Get query data
    query = open(f'./sql_queries/{j}', 'r').read()
    df = rs.get_result_df(sql_query=query)

    # Close Redshift connection
    rs.conn.close()

    # Create percentage variables
    df['attrition_pct'] = df['attrition_cnt']/df['actived_acct_cnt']
    df['co_pct'] = df['co_cnt']/ df['actived_acct_cnt']
    df['retention_pct'] = df['open_acct_cnt']/ df['actived_acct_cnt']
    df['revolving_pct'] = df['tot_adb_amt']/ df['all_adb_amt']
    df['active_rate_2'] = df['active_acct_cnt']/df['actived_acct_cnt']

    # Create pivot tables
    actived_acct_cnt = pd.pivot_table(df,values='actived_acct_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    attrition_cnt = pd.pivot_table(df,values='attrition_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    co_cnt = pd.pivot_table(df,values='co_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    other_close_cnt = pd.pivot_table(df,values='other_close_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    open_acct_cnt = pd.pivot_table(df,values='open_acct_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    retention_pct = pd.pivot_table(df,values='retention_pct',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    retention_pct.iloc[-1] = open_acct_cnt.iloc[-1]/actived_acct_cnt.iloc[-1]
    actived_credit_limit = pd.pivot_table(df,values='actived_credit_limit',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    open_credit_limit = pd.pivot_table(df,values='open_credit_limit',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    active_credit_limit = pd.pivot_table(df,values='active_credit_limit',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    avg_active_credit_line = pd.pivot_table(df,values='avg_active_credit_line',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    active_acct_cnt = pd.pivot_table(df,values='active_acct_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    avg_active_credit_line.iloc[-1] = active_credit_limit.iloc[-1]/active_acct_cnt.iloc[-1]
    active_rate = pd.pivot_table(df,values='active_rate',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    active_rate.iloc[-1] = active_acct_cnt.iloc[-1]/open_acct_cnt.iloc[-1]
    transactor_acct_cnt = pd.pivot_table(df,values='transactor_acct_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    transactor_acct_pct = pd.pivot_table(df,values='transactor_acct_pct',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    transactor_acct_pct.iloc[-1] =  transactor_acct_cnt.iloc[-1]/open_acct_cnt.iloc[-1]
    revolver_acct_cnt = pd.pivot_table(df,values='revolver_acct_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    revolver_acct_pct = pd.pivot_table(df,values='revolver_acct_pct',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    revolver_acct_pct.iloc[-1] = revolver_acct_cnt.iloc[-1]/open_acct_cnt.iloc[-1]
    tot_end_os_bal_amt = pd.pivot_table(df,values='tot_end_os_bal_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_end_os_bal_amt = pd.pivot_table(df,values='acct_end_os_bal_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_end_os_bal_amt.iloc[-1] = tot_end_os_bal_amt.iloc[-1]/active_acct_cnt.iloc[-1]
    tot_txn_amt = pd.pivot_table(df,values='tot_txn_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_txn_amt = pd.pivot_table(df,values='acct_txn_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_txn_amt.iloc[-1] = tot_txn_amt.iloc[-1]/active_acct_cnt.iloc[-1]
    txn_cl_util_pct = pd.pivot_table(df,values='txn_cl_util_pct',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    txn_cl_util_pct.iloc[-1] = tot_txn_amt.iloc[-1]/active_credit_limit.iloc[-1]
    tot_txn_cnt = pd.pivot_table(df,values='tot_txn_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_txn_cnt = pd.pivot_table(df,values='acct_txn_cnt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_txn_cnt.iloc[-1] = tot_txn_cnt.iloc[-1]/active_acct_cnt.iloc[-1]
    txn_size_amt = pd.pivot_table(df,values='txn_size_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    txn_size_amt.iloc[-1] = tot_txn_amt.iloc[-1]/tot_txn_cnt.iloc[-1]
    tot_adb_amt = pd.pivot_table(df,values='tot_adb_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_adb_amt = pd.pivot_table(df,values='acct_adb_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    acct_adb_amt.iloc[-1] = tot_adb_amt.iloc[-1]/active_acct_cnt.iloc[-1]
    adb_cl_util_pct = pd.pivot_table(df,values='adb_cl_util_pct',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    adb_cl_util_pct.iloc[-1] = tot_adb_amt.iloc[-1]/active_credit_limit.iloc[-1]
    tot_prncp_amt = pd.pivot_table(df,values='tot_prncp_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    all_adb = pd.pivot_table(df,values='all_adb_amt',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    revolving_pct = pd.pivot_table(df,values='revolving_rate',index='vintage',columns='stmt_mth',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    revolving_pct.iloc[-1] = tot_adb_amt.iloc[-1]/all_adb.iloc[-1]
# metrics lists, list with names, zip return dict

def write_tables_to_excel(sql_query,sheet_name='sheet1',mode_default='w'):
    dict_df = create_pivot_tables(sql_query)
    startR,startC = 1,1
    file_name = './portfolio_usage_pivot_' + str(datetime.date.today()) + '.xlsx'
    with pd.ExcelWriter(file_name,mode=mode_default,engine="openpyxl") as writer:
        for k,v in dict_df.items():
            v.to_excel(writer, sheet_name=sheet_name,startrow=startR,startcol=startC,index_label=k)
            startR += v.shape[0]+6

if __name__ == "__main__":
    query = open('./sql_queries/usage_pivot.sql', 'r').read()
    
    write_tables_to_excel(sql_queries=query,sheet_name='portfolio_usage_pivot')