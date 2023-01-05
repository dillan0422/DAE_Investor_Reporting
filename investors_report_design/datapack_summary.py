# Import libraries
import os
import datetime
import pandas as pd
import openpyxl
from util import rs_conn

def get_portfolio_data(sql_queries):
    # Create dictionary with empty DataFrames
    tab_name = [x.split('.')[0] for x in sql_queries]
    tabs_dict = dict(('' + str(i), pd.DataFrame()) for i in tab_name)

    # Get the redshift user name and password
    rs_user = os.getenv('RS_USER')
    rs_pass = os.getenv('RS_PASS')

    # Connect to the Redshift
    rs = rs_conn.RS_CONN(rs_user=rs_user, rs_pass=rs_pass)
    rs.set_Conn()

    # Get query data
    for i,j in zip(tabs_dict.keys(), sql_queries):
        query = open(f'./sql_queries/{j}', 'r').read()
        df = rs.get_result_df(sql_query=query)
        tabs_dict[i] = (df.T)

    # Close Redshift connection
    rs.conn.close()

    return tabs_dict

def write_tables_to_excel(sql_queries,sheet_name='sheet1',mode_default='w'):
    dict_df = get_portfolio_data(sql_queries)
    startR,startC = 1,1
    file_name = './portfolio_summary_' + str(datetime.date.today()) + '.xlsx'
    with pd.ExcelWriter(file_name,mode=mode_default,engine="openpyxl") as writer:
        for k,v in dict_df.items():
            v.to_excel(writer, sheet_name=sheet_name,startrow=startR,startcol=startC,index_label=k)
            startR += v.shape[0]+6

if __name__ == "__main__":
    path = os.getcwd() + '/sql_queries'
    all_files = os.listdir(path)
    sql_files = list(filter(lambda f: f.endswith("summary.sql"), all_files))
    
    write_tables_to_excel(sql_queries=sql_files,sheet_name='portfolio_summary')