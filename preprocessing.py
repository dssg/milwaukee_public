import pandas as pd

'''
TODO: bunch of functions
	  each works on a feature's column does some preprocessing in pandas
	  returns entire dataframe with updated column
	  the features get to decide which of the functions here they would like to invoke
'''

# def exampleprocessor(data,**kwargs):
# 	'''
# 	@param data: huge dataframe with all the data
# 	@param kwargs: all kinds of more fun parameters you might need!
# 	@return: modified version of data
# 	'''
# 	pass

def get_dummies(data, columns = [], **kwargs):
    #data.loc[data[columns].str.contains(' '), columns] = data[columns].str.replace(' ', '_')
    #return pd.get_dummies(df['itemID'],prefix = 'itemID_').astype(np.int8)
    return pd.get_dummies(data, columns = columns,**kwargs)


def fill_null_with_mean(data, columns):
    data = data.fillna(data.mean()[columns])
    return data


def fill_null_with_median(data, columns):
    data = data.fillna(data.median()[columns])
    return data

def fill_null_with_default_value(data, columns, value):
    data.loc[:, columns] = data[columns].fillna(value)
    return data

def dummy_code_null(data, columns):

    for c in columns:
        if data[c].isnull().sum() > 0:
            data[c + '_isnull'] = (data[c].isnull()).astype(int)
    
    data.loc[:, columns] = data[columns].fillna(0)
    return data


def fill_null_with_zero(data, columns):
    data.loc[:, columns] = data[columns].fillna(0)
    return data


if __name__=='__main__':
    pass
