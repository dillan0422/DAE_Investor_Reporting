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
    metrics_names = ['total_revenue','arpu','adj_interest_revenue_amt','interchange_revenue_amt','open_fee_rev_amt','fee_revenue_amt','service_pmt_revenue_amt']

    # Create variables
    df["arpu"] = df["total_revenue"]/df["active_acct_cnt"]

    # Create pivot tables
    total_revenue = pd.pivot_table(df,values='total_revenue',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    arpu = pd.pivot_table(df,values='arpu',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    adj_interest_revenue_amt = pd.pivot_table(df,values='adj_interest_revenue_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    interchange_revenue_amt = pd.pivot_table(df,values='interchange_revenue_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    open_fee_rev_amt = pd.pivot_table(df,values='open_fee_rev_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    fee_revenue_amt = pd.pivot_table(df,values='fee_revenue_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)
    service_pmt_revenue_amt = pd.pivot_table(df,values='service_pmt_revenue_amt',index='vintage',columns='cycle_end_dt',aggfunc='sum',margins=True,margins_name = 'Total').drop('Total',axis=1)

    metrics_list = [total_revenue,arpu,adj_interest_revenue_amt,interchange_revenue_amt,open_fee_rev_amt,fee_revenue_amt,service_pmt_revenue_amt]
    metrics_dict = dict(zip(metrics_names, metrics_list))

    return metrics_dict

# Metrics format
def format_amt(val):
    return 'number-format: $#,##0' 

# Function to write pivot tables in an Excel Spreadsheet
def write_tables_to_excel(sql_query,sheet_name='sheet1',mode_default='w'):
    dict_df = create_pivot_tables(sql_query)
    startR,startC = 1,1
    file_name = './datapack_spreadsheets/portfolio_revenue_pivot_' + str(datetime.date.today()) + '.xlsx'
    with pd.ExcelWriter(file_name,mode=mode_default,engine="openpyxl") as writer:
        for k,v in dict_df.items():
            v.style.applymap(format_amt).to_excel(writer, sheet_name=sheet_name,startrow=startR,startcol=startC,index_label=k)
            startR += v.shape[0]+6

if __name__ == "__main__":
    query = open('./sql_queries/revenue_pivot.sql', 'r').read()
    
    write_tables_to_excel(sql_query=query,sheet_name='portfolio_revenue_pivot')

    if os.path.exists('./datapack_spreadsheets/portfolio_revenue_pivot_' + str(datetime.date.today()) + '.xlsx'):
        print('<< Portfolio revenue pivot spreadsheet successfully created >>')
    else:
        print('<< Error creating portfolio revenue pivot spreadsheet >>')