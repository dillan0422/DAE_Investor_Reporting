# Import libraries
import os
import datetime
import pandas as pd
import openpyxl
from util import rs_conn

# Function to create pivot tables
def create_pivot_tables(sql_query):
    # Get the redshift user name and password
    rs_user = os.getenv('RS_USER')
    rs_pass = os.getenv('RS_PASS')

    # Connect to the Redshift
    rs = rs_conn.RS_CONN(rs_user=rs_user, rs_pass=rs_pass)
    rs.set_Conn()

    # Get query data
    df = rs.get_result_df(sql_query=query)

    # Close Redshift connection
    rs.conn.close()

    # Metrics names
    metrics_names = ['total_os_bal','dq1_30_prncp_bal','dq31_60_prncp_bal','dq61_90_prncp_bal','co_amt','dq1_30_prncp_bal_pct','dq31_60_prncp_bal_pct','dq61_90_prncp_bal_pct','naco','dq91_180_prncp_bal','dq91_180_prncp_bal_pct',
    'stmt_total_os_bal','stmt_dq1_30_prncp_bal','stmt_dq31_60_prncp_bal','stmt_dq61_90_prncp_bal','stmt_co_amt','stmt_dq1_30_prncp_bal_pct','stmt_dq31_60_prncp_bal_pct','stmt_dq61_90_prncp_bal_pct','stmt_naco','stmt_dq91_180_prncp_bal','stmt_dq91_180_prncp_bal_pct']

    # Create percentage variables
    df['dq1_30_prncp_bal_pct'] = df['dq1_30_prncp_bal']/df['os_bal']
    df['dq31_60_prncp_bal_pct'] = df['dq31_60_prncp_bal']/df['os_bal']
    df['dq61_90_prncp_bal_pct'] = df['dq61_90_prncp_bal']/df['os_bal']
    df['dq91_180_prncp_bal_pct'] = df['dq91_180_prncp_bal']/df['os_bal']
    df['naco'] = df['co_amt']/df['os_bal']

    # Create pivot tables
    total_os_bal = pd.pivot_table(df,values='os_bal',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq1_30_prncp_bal = pd.pivot_table(df,values='dq1_30_prncp_bal',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq31_60_prncp_bal = pd.pivot_table(df,values='dq31_60_prncp_bal',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq61_90_prncp_bal = pd.pivot_table(df,values='dq61_90_prncp_bal',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    co_amt = pd.pivot_table(df,values='co_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq1_30_prncp_bal_pct = pd.pivot_table(df,values='dq1_30_prncp_bal_pct',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq1_30_prncp_bal_pct.iloc[-1] = dq1_30_prncp_bal.iloc[-1]/total_os_bal.iloc[-1]
    dq31_60_prncp_bal_pct = pd.pivot_table(df,values='dq31_60_prncp_bal_pct',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq31_60_prncp_bal_pct.iloc[-1] = dq31_60_prncp_bal.iloc[-1]/total_os_bal.iloc[-1]
    dq61_90_prncp_bal_pct = pd.pivot_table(df,values='dq61_90_prncp_bal_pct',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq61_90_prncp_bal_pct.iloc[-1] = dq61_90_prncp_bal.iloc[-1]/total_os_bal.iloc[-1]
    naco = pd.pivot_table(df,values='naco',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    naco.iloc[-1] = naco.iloc[-1]/total_os_bal.iloc[-1]
    dq91_180_prncp_bal = pd.pivot_table(df,values='dq91_180_prncp_bal',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq91_180_prncp_bal_pct = pd.pivot_table(df,values='dq91_180_prncp_bal_pct',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    dq91_180_prncp_bal_pct.iloc[-1] = dq91_180_prncp_bal.iloc[-1]/total_os_bal.iloc[-1]
    
    stmt_total_os_bal = pd.pivot_table(df,values='os_bal',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq1_30_prncp_bal = pd.pivot_table(df,values='dq1_30_prncp_bal',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq31_60_prncp_bal = pd.pivot_table(df,values='dq31_60_prncp_bal',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq61_90_prncp_bal = pd.pivot_table(df,values='dq61_90_prncp_bal',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_co_amt = pd.pivot_table(df,values='co_amt',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq1_30_prncp_bal_pct = pd.pivot_table(df,values='dq1_30_prncp_bal_pct',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq1_30_prncp_bal_pct.iloc[-1] = stmt_dq1_30_prncp_bal.iloc[-1]/stmt_total_os_bal.iloc[-1]
    stmt_dq31_60_prncp_bal_pct = pd.pivot_table(df,values='dq31_60_prncp_bal_pct',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq31_60_prncp_bal_pct.iloc[-1] = stmt_dq31_60_prncp_bal.iloc[-1]/stmt_total_os_bal.iloc[-1]
    stmt_dq61_90_prncp_bal_pct = pd.pivot_table(df,values='dq61_90_prncp_bal_pct',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq61_90_prncp_bal_pct.iloc[-1] = stmt_dq61_90_prncp_bal.iloc[-1]/stmt_total_os_bal.iloc[-1]
    stmt_naco = pd.pivot_table(df,values='naco',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_naco.iloc[-1] = stmt_naco.iloc[-1]/stmt_total_os_bal.iloc[-1]
    stmt_dq91_180_prncp_bal = pd.pivot_table(df,values='dq91_180_prncp_bal',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq91_180_prncp_bal_pct = pd.pivot_table(df,values='dq91_180_prncp_bal_pct',index='vintage',columns='stmt_num',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    stmt_dq91_180_prncp_bal_pct.iloc[-1] = stmt_dq91_180_prncp_bal.iloc[-1]/stmt_total_os_bal.iloc[-1]
    
    metrics_list = [total_os_bal,dq1_30_prncp_bal,dq31_60_prncp_bal,dq61_90_prncp_bal,co_amt,dq1_30_prncp_bal_pct,dq31_60_prncp_bal_pct,dq61_90_prncp_bal_pct,naco,dq91_180_prncp_bal,dq91_180_prncp_bal_pct,
    stmt_total_os_bal,stmt_dq1_30_prncp_bal,stmt_dq31_60_prncp_bal,stmt_dq61_90_prncp_bal,stmt_co_amt,stmt_dq1_30_prncp_bal_pct,stmt_dq31_60_prncp_bal_pct,stmt_dq61_90_prncp_bal_pct,stmt_naco,stmt_dq91_180_prncp_bal,stmt_dq91_180_prncp_bal_pct]

    metrics_dict = dict(zip(metrics_names, metrics_list))

    return metrics_dict

# Metrics format

pct = ['dq1_30_prncp_bal_pct','dq31_60_prncp_bal_pct','dq61_90_prncp_bal_pct','naco','dq91_180_prncp_bal_pct','stmt_dq1_30_prncp_bal_pct','stmt_dq31_60_prncp_bal_pct','stmt_dq61_90_prncp_bal_pct','stmt_naco','stmt_dq91_180_prncp_bal_pct']
amt = ['total_os_bal','dq1_30_prncp_bal','dq31_60_prncp_bal','dq61_90_prncp_bal','co_amt','dq91_180_prncp_bal','stmt_total_os_bal','stmt_dq1_30_prncp_bal','stmt_dq31_60_prncp_bal','stmt_dq61_90_prncp_bal','stmt_co_amt','stmt_dq91_180_prncp_bal']

def format_amt(val):
    return 'number-format: $#,##0'

def format_pct(val):
    return 'number-format: 0.0%'   

# Function to write pivot tables in an Excel Spreadsheet
def write_tables_to_excel(sql_query,sheet_name='sheet1',mode_default='w'):
    dict_df = create_pivot_tables(sql_query)
    startR,startC = 1,1
    file_name = './datapack_spreadsheets/portfolio_dq_pivot_' + str(datetime.date.today()) + '.xlsx'
    with pd.ExcelWriter(file_name,mode=mode_default,engine="openpyxl") as writer:
        for k,v in dict_df.items():
            if k in pct:
                v.style.applymap(format_pct).to_excel(writer, sheet_name=sheet_name,startrow=startR,startcol=startC,index_label=k)
            else:
                v.style.applymap(format_amt).to_excel(writer, sheet_name=sheet_name,startrow=startR,startcol=startC,index_label=k)
            startR += v.shape[0]+6

if __name__ == "__main__":
    query = open('./sql_queries/dq_pivot.sql', 'r').read()
    
    write_tables_to_excel(sql_query=query,sheet_name='portfolio_dq_pivot')

    if os.path.exists('./datapack_spreadsheets/portfolio_dq_pivot_' + str(datetime.date.today()) + '.xlsx'):
        print('<< Portfolio dq pivot spreadsheet successfully created >>')
    else:
        print('<< Error creating portfolio dq pivot spreadsheet >>')