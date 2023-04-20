import pandas
pd.options.mode.chained_assignment = None

def average_booked_balance_from(transactions: pandas.DataFrame,
                                accounts: pandas.DataFrame,
                                reference_timestamps: pandas.DataFrame) -> pandas.Series:
    """
    :param transactions: pandas dataframe containing the transactions from a collection of accounts
    :param accounts: pandas dataframe containing a collection of accounts together with their balance when they
        were first added to our systems.
    :param reference_timestamps: pandas dataframe with the timestamp a which to compute the average booked balance for
        each account. Different account might have different reference timestamps.
    :return:
        a pandas series where the index is a multindex containing the reference timestamp and the account id, and the
        values are the average booked balances, e.g
        index                               | value
        ('2022-01-12 23:59:59.999', 'ac_1') | 12.3
        ('2022-03-10 23:59:59.999', 'ac_2') | 26.8
    """
    # transform date to datetime format
    accounts['creation_timestamp'] = pd.to_datetime(accounts['creation_timestamp'])
    transactions['value_date'] = pd.to_datetime(transactions['value_date'])
    reference_timestamps['reference_timestamp']=pd.to_datetime(reference_timestamps['reference_timestamp'])
    
    #rename column creation_timestamp of acc to merge it with trans
    accounts=accounts.rename(columns={"creation_timestamp": "value_date"})
    
    #order and group by account and value date
    df_t=transactions.sort_values(by=['account_id', 'value_date']).groupby(['account_id', 'value_date']).sum()
    df_a=accounts.groupby(['account_id', 'value_date']).sum()

    df1=pd.concat([df_t, df_a])
    df1=df1.sort_index(level=['account_id', 'value_date'])

    #list of accounts to interate
    account_ids=np.unique(df1.index.get_level_values('account_id')).tolist()
 
    reference_timestamps['result']=0
    ab_dict={}

    for id in account_ids:

        #select one account at time
        df2=df1[df1.index.get_level_values('account_id')==id]

        df2['amount_cumsum']=0
        df2['delta']=0

        #compute sum of transactions. at the moment in which balance is available, cumsum=balance
        for i in range(0, len(df2)):
            if np.isnan(df2.iloc[i, df2.columns.get_loc('amount')])==False:
                if i==0:
                    df2.iloc[i,df2.columns.get_loc('amount_cumsum')]=df2.iloc[i,df2.columns.get_loc('amount')]
                else:
                    df2.iloc[i,df2.columns.get_loc('amount_cumsum')]=df2.iloc[i-1,df2.columns.get_loc('amount_cumsum')]+df2.iloc[i,df2.columns.get_loc('amount')]
            else:
                df2.iloc[i,df2.columns.get_loc('amount_cumsum')]=df2.iloc[i,df2.columns.get_loc('balance_at_creation')]

            #compute number of days between one date and the other    
            if i<len(df2)-1:
                df2.iloc[i, df2.columns.get_loc('delta')] = max((df2.index.get_level_values('value_date')[i+1]-df2.index.get_level_values('value_date')[i]).days,0)


        #filter for dates only in range 
        start_date=pd.to_datetime((reference_timestamps[reference_timestamps['account_id']==id].reference_timestamp- np.timedelta64(89, 'D')).values[0])
        end_date=pd.to_datetime((reference_timestamps[reference_timestamps['account_id']==id].reference_timestamp).values[0])

        df4=df2[df2.index.get_level_values('value_date')>=start_date]
        df3=df4[df4.index.get_level_values('value_date')<=end_date]

        df3.iloc[-1, -1]=(end_date-df3.index.get_level_values('value_date')[-1]).days

        #add first available value 
        if df2.index.get_level_values('value_date')[0]<start_date:
            last_sum=df2[df2.index.get_level_values('value_date')<start_date].iloc[-1].amount_cumsum
        else:
            last_sum=df3.iloc[0].amount_cumsum

        first_delta=(df3.index.get_level_values('value_date')[0]-start_date).days

        average_balance=(first_delta*last_sum+np.multiply(df3['amount_cumsum'], df3['delta']).sum())/90

        ab_dict[id]=average_balance
        
        
        reference_timestamps['result'] = reference_timestamps['account_id'].map(ab_dict)
        
        results=reference_timestamps.set_index(['account_id','reference_timestamp']).drop('average_booked_balance', axis=1).squeeze()
    
    return results
