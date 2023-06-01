import pandas as pd
import numpy as np
pd.options.mode.chained_assignment = None

#import data
accounts=pd.read_csv('account_booked_balance_mean_3mo_accounts.csv')
reference_timestamps=pd.read_csv("account_booked_balance_mean_3mo_results.csv")
transactions=pd.read_csv("account_booked_balance_mean_3mo_transactions.csv")

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
    transaction_sorted=transactions.sort_values(by=['account_id', 'value_date']).groupby(['account_id', 'value_date'], as_index=False).sum()
    accounts_sorted=accounts.groupby(['account_id', 'value_date'], as_index=False).sum()

    transactions_from_single_account=pd.concat([transaction_sorted, accounts_sorted])
    transactions_from_single_account=transactions_from_single_account.sort_values(['account_id', 'value_date'])

    #list of accounts to interate
    account_ids=set(accounts['account_id'])
 
    reference_timestamps['result']=0
    ab_dict={}
     
    for id in account_ids:
        transaction_from_id=transactions_from_single_account[transactions_from_single_account['account_id']==id].reset_index().drop('index', axis=1)
        transaction_from_id['amount_cumsum']=0
        transaction_from_id['delta']=0

        #compute sum of transactions. at the moment in which balance is available, cumsum=balance
        reference_timestamps_id=reference_timestamps[reference_timestamps['account_id']==id]
        start_date=pd.to_datetime((reference_timestamps_id.reference_timestamp- np.timedelta64(89, 'D')).values[0])
        end_date=pd.to_datetime((reference_timestamps_id.reference_timestamp).values[0])

        for i,row in transaction_from_id.iterrows():
            if np.isnan(row['amount'])==True:
                transaction_from_id.loc[i,'amount_cumsum']=transaction_from_id.loc[i,'balance_at_creation']
            else:
                transaction_from_id.loc[i,'amount_cumsum']=transaction_from_id.loc[i,'amount'] if i==0 else transaction_from_id.loc[i,'amount']+transaction_from_id.loc[i - 1, 'amount_cumsum']

            if i<len(transaction_from_id)-1:
                transaction_from_id.loc[i,'delta']=max((transaction_from_id.loc[i +1, 'value_date']-transaction_from_id.loc[i,'value_date']).days,0)

        transaction_in_range=transaction_from_id[(transaction_from_id['value_date']>=start_date) & (transaction_from_id['value_date']<=end_date)]
        transaction_in_range.iloc[-1, -1]=(end_date-transaction_in_range.value_date.iloc[-1]).days

        #add first available value 
        if transaction_from_id.value_date.iloc[0] < start_date:
            last_sum=transaction_from_id[transaction_from_id['value_date']<start_date].iloc[-1].amount_cumsum
        else:
            last_sum=transaction_in_range.iloc[0].amount_cumsum


        first_delta=(transaction_in_range['value_date'].iloc[0]-start_date).days

        average_balance=(first_delta*last_sum+np.multiply(transaction_in_range['amount_cumsum'], transaction_in_range['delta']).sum())/90

        ab_dict[id]=average_balance

        reference_timestamps['result'] = reference_timestamps['account_id'].map(ab_dict)

        results=reference_timestamps.set_index(['account_id','reference_timestamp']).drop('average_booked_balance', axis=1).squeeze()
    
    return results
